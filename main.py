from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import os
import json
import bcrypt
import jwt
import threading
import time as time_mod
import schedule
import requests
import urllib.request
import urllib.parse
import urllib.error
from contextlib import contextmanager
from typing import Optional, List

# ── ENV VARS (set in Render) ──────────────────────────────────────────────────

DATABASE_URL = os.environ.get(“DATABASE_URL”, “”)
if not DATABASE_URL:
raise RuntimeError(
“DATABASE_URL environment variable not set. “
“In Render, link a Postgres database to this service or set DATABASE_URL manually.”
)

# Render sometimes uses postgres:// (legacy). psycopg2 accepts both, but normalize for safety.

if DATABASE_URL.startswith(“postgres://”):
DATABASE_URL = DATABASE_URL.replace(“postgres://”, “postgresql://”, 1)

SECRET_KEY = os.environ.get(“SECRET_KEY”, os.environ.get(“JWT_SECRET”, “ohw-fallback-change-me”))
OPENFDA_KEY = os.environ.get(“OPENFDA_KEY”, “”)
RESEND_API_KEY = os.environ.get(“RESEND_API_KEY”, “”)
FROM_EMAIL = os.environ.get(“FROM_EMAIL”, “alerts@ourhealth.watch”)

# v0.1.7: geocoding for place-based watchlist (Search by region/state/city).

GOOGLE_GEOCODING_API_KEY = os.environ.get(“GOOGLE_GEOCODING_API_KEY”, “”)

API_VERSION = “0.1.8”
JWT_ALGO = “HS256”
JWT_EXPIRY_DAYS = 7
WATCHLIST_CHECK_INTERVAL_HOURS = 12  # free tier; premium will be 1hr
INGEST_WINDOW_DAYS = 30  # rolling window for openFDA drug+device recalls
OPENFDA_DRUG_URL = “https://api.fda.gov/drug/enforcement.json”
OPENFDA_DEVICE_URL = “https://api.fda.gov/device/enforcement.json”
NORS_URL = “https://data.cdc.gov/resource/5xkq-dg7x.json”  # CDC NORS foodborne/waterborne outbreaks
NORS_FETCH_LIMIT = 200

# v0.1.7: same 12-region taxonomy as Cruise Ship Watch / EarthWatch — covers the Earth.

OH_REGIONS = [
“Africa”, “Alaska”, “Arctic”, “Asia”, “Caribbean”, “Central America”,
“Mediterranean”, “Middle East”, “North America”, “Northern Europe”,
“Oceania”, “South America”,
]

app = FastAPI(title=“OurHealth.Watch API”, version=API_VERSION)

app.add_middleware(
CORSMiddleware,
allow_origins=[”*”],
allow_credentials=True,
allow_methods=[”*”],
allow_headers=[”*”],
)

# ── DB HELPERS ────────────────────────────────────────────────────────────────

@contextmanager
def get_db():
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
try:
yield conn
finally:
conn.close()

def init_db():
try:
with get_db() as conn:
c = conn.cursor()

```
        # pg_trgm enables fuzzy similarity for future /suggest endpoint
        try:
            c.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        except Exception as ex:
            print("[init_db] pg_trgm note: " + str(ex))

        # v0.1.7: PostGIS for place-based watchlist (lat/lng geofences).
        # Idempotent — safe to run on every boot.
        try:
            c.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        except Exception as ex:
            print("[init_db] postgis note: " + str(ex))

        # ── oh_users ──────────────────────────────────────────────────────
        c.execute("""CREATE TABLE IF NOT EXISTS oh_users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # ── oh_watchlist ──────────────────────────────────────────────────
        # Generic shape — accommodates recalls (brand/product/upc),
        # outbreaks (agent/region), advisories (country), etc.
        # Add columns via ALTER ... ADD COLUMN IF NOT EXISTS as new
        # adapters land. v0.1.0 shape mirrors MW for recall compatibility.
        c.execute("""CREATE TABLE IF NOT EXISTS oh_watchlist (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            kind TEXT DEFAULT 'product',
            brand TEXT,
            product_name TEXT,
            upc TEXT,
            keyword TEXT,
            category TEXT,
            monitoring BOOLEAN DEFAULT TRUE,
            has_alert BOOLEAN DEFAULT FALSE,
            last_match_id TEXT,
            last_checked TIMESTAMP,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES oh_users (id)
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_watchlist_user ON oh_watchlist (user_id)")

        # ── oh_notifications ──────────────────────────────────────────────
        c.execute("""CREATE TABLE IF NOT EXISTS oh_notifications (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            watchlist_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            source TEXT,
            source_ref_id TEXT,
            email_sent BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES oh_users (id),
            FOREIGN KEY (watchlist_id) REFERENCES oh_watchlist (id)
        )""")

        # ── oh_recalls ────────────────────────────────────────────────────
        # Generic recall store; populated by openFDA drug ingest in v0.1.1,
        # then device in v0.1.2. Source field distinguishes feeds.
        c.execute("""CREATE TABLE IF NOT EXISTS oh_recalls (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            recall_id TEXT UNIQUE NOT NULL,
            brand TEXT,
            product_description TEXT,
            upc TEXT,
            classification TEXT,
            reason TEXT,
            recall_date TEXT,
            distribution TEXT,
            lot_codes TEXT,
            status TEXT,
            raw_json TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_recalls_brand ON oh_recalls (brand)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_recalls_source ON oh_recalls (source)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_recalls_status ON oh_recalls (status)")

        # ── oh_outbreaks ──────────────────────────────────────────────────
        # Reserved schema for VSP cruise outbreaks, NORS foodborne/waterborne,
        # WHO DON entries. Source field distinguishes. Adapters land v0.1.4+.
        c.execute("""CREATE TABLE IF NOT EXISTS oh_outbreaks (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            outbreak_id TEXT UNIQUE NOT NULL,
            title TEXT,
            agent TEXT,
            location TEXT,
            country_code TEXT,
            region TEXT,
            ship_name TEXT,
            cruise_line TEXT,
            cases INTEGER,
            report_date TEXT,
            report_url TEXT,
            summary TEXT,
            raw_json TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_outbreaks_source ON oh_outbreaks (source)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_outbreaks_agent ON oh_outbreaks (agent)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_outbreaks_region ON oh_outbreaks (region)")

        # ── oh_ingest_log ─────────────────────────────────────────────────
        # Per-source last-check tracking. Powers the "Last checked X min ago"
        # timestamp shown in every list view and empty state. One row per
        # source. Updated on every successful ingest, success or empty.
        c.execute("""CREATE TABLE IF NOT EXISTS oh_ingest_log (
            source TEXT PRIMARY KEY,
            last_checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_success_at TIMESTAMP,
            last_record_count INTEGER DEFAULT 0,
            last_error TEXT,
            total_records INTEGER DEFAULT 0
        )""")

        # ── oh_places ─────────────────────────────────────────────────────
        # v0.1.7: the place-based watchlist primitive. Mirrors EW's ew_places
        # exactly, with region/state/city columns added for the OHW search UX
        # (Search by region, then optionally narrow by state and city).
        #
        # Two booleans, identical semantics to EW v0.1.6+:
        #   - is_archived: legacy flag, kept for backward-compat.
        #   - in_my_places: v0.1.7 model. A place ALWAYS stays in Watchlist
        #     (where the cron monitors it). "Save to My Places" flips this
        #     flag TRUE without removing from Watchlist. Watchlist and
        #     My Places are independent views of the same row.
        c.execute("""CREATE TABLE IF NOT EXISTS oh_places (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            region TEXT,
            country TEXT,
            country_code TEXT,
            state TEXT,
            city TEXT,
            formatted_address TEXT,
            lat DOUBLE PRECISION NOT NULL,
            lng DOUBLE PRECISION NOT NULL,
            radius_mi DOUBLE PRECISION NOT NULL DEFAULT 50,
            alert_level TEXT NOT NULL DEFAULT 'realtime',
            is_archived BOOLEAN NOT NULL DEFAULT FALSE,
            in_my_places BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES oh_users (id)
        )""")
        # v0.1.8: idempotent column adds for upgrade from v0.1.7 schema.
        c.execute("ALTER TABLE oh_places ADD COLUMN IF NOT EXISTS country TEXT")
        c.execute("ALTER TABLE oh_places ADD COLUMN IF NOT EXISTS country_code TEXT")
        c.execute("ALTER TABLE oh_places ADD COLUMN IF NOT EXISTS formatted_address TEXT")
        # Generated PostGIS geometry column (auto-derived from lat/lng).
        c.execute("""DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='oh_places' AND column_name='geom'
            ) THEN
                ALTER TABLE oh_places
                ADD COLUMN geom geography(Point, 4326)
                GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography) STORED;
            END IF;
        END $$;""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_places_user ON oh_places (user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_places_geom ON oh_places USING GIST (geom)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_places_region ON oh_places (region)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_places_state ON oh_places (state)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_oh_places_country_code ON oh_places (country_code)")

        conn.commit()
        print("[init_db] tables ready (oh_users, oh_watchlist, oh_notifications, oh_recalls, oh_outbreaks, oh_ingest_log, oh_places)")
except Exception as e:
    print("[init_db] WARNING: " + str(e))
```

def update_ingest_log(source: str, success: bool, record_count: int = 0, error: Optional[str] = None):
“”“Update per-source ingest log. Called by every adapter on every run.”””
try:
with get_db() as conn:
c = conn.cursor()
now = datetime.utcnow()
if success:
c.execute(””“INSERT INTO oh_ingest_log (source, last_checked_at, last_success_at, last_record_count, last_error, total_records)
VALUES (%s, %s, %s, %s, NULL, %s)
ON CONFLICT (source) DO UPDATE SET
last_checked_at = EXCLUDED.last_checked_at,
last_success_at = EXCLUDED.last_success_at,
last_record_count = EXCLUDED.last_record_count,
last_error = NULL,
total_records = oh_ingest_log.total_records + EXCLUDED.last_record_count”””,
(source, now, now, record_count, record_count))
else:
c.execute(””“INSERT INTO oh_ingest_log (source, last_checked_at, last_error)
VALUES (%s, %s, %s)
ON CONFLICT (source) DO UPDATE SET
last_checked_at = EXCLUDED.last_checked_at,
last_error = EXCLUDED.last_error”””,
(source, now, error or “unknown error”))
conn.commit()
except Exception as e:
print(”[update_ingest_log] failed for “ + source + “: “ + str(e))

def get_ingest_status() -> dict:
“”“Return all source last-check timestamps for /health and empty-state UX.”””
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
c.execute(“SELECT source, last_checked_at, last_success_at, last_record_count, last_error, total_records FROM oh_ingest_log”)
rows = c.fetchall()
out = {}
for r in rows:
out[r[“source”]] = {
“last_checked_at”: r[“last_checked_at”].isoformat() if r[“last_checked_at”] else None,
“last_success_at”: r[“last_success_at”].isoformat() if r[“last_success_at”] else None,
“last_record_count”: r[“last_record_count”],
“last_error”: r[“last_error”],
“total_records”: r[“total_records”],
}
return out
except Exception as e:
print(”[get_ingest_status] failed: “ + str(e))
return {}

# ── INGEST: openFDA DRUG RECALLS ──────────────────────────────────────────────

def ingest_openfda_drugs(window_days: int = INGEST_WINDOW_DAYS) -> dict:
“”“Pull drug recalls from openFDA /drug/enforcement.json over a rolling
window. Upserts into oh_recalls with source=‘fda_drug’. Updates oh_ingest_log
on every run, success or failure. Mirrors MW’s openFDA pattern for food.”””
cutoff = (datetime.utcnow() - timedelta(days=window_days)).strftime(”%Y%m%d”)
today = datetime.utcnow().strftime(”%Y%m%d”)
# NOTE: openFDA needs literal ‘+TO+’ in the search string; build URL manually
# since requests will URL-encode the ‘+’ as ‘%2B’ if passed via params.
search_str = “report_date:[” + cutoff + “+TO+” + today + “]”
full_url = OPENFDA_DRUG_URL + “?search=” + search_str + “&limit=1000”
if OPENFDA_KEY:
full_url = full_url + “&api_key=” + OPENFDA_KEY
headers = {“User-Agent”: “Mozilla/5.0 (ourhealthwatch/0.1.1)”}
inserted = 0
skipped = 0
try:
r = requests.get(full_url, headers=headers, timeout=30)
if r.status_code != 200:
err = “HTTP “ + str(r.status_code) + “ “ + r.text[:200]
update_ingest_log(“fda_drug”, success=False, error=err)
return {“source”: “fda_drug”, “error”: err, “inserted”: 0}
data = r.json()
results = data.get(“results”, [])
with get_db() as conn:
c = conn.cursor()
for rec in results:
rid = “fda_drug_” + (rec.get(“recall_number”) or rec.get(“event_id”) or “”)
if rid == “fda_drug_”:
skipped = skipped + 1
continue
brand = (rec.get(“recalling_firm”) or “”).strip()
desc = (rec.get(“product_description”) or “”).strip()
cls = (rec.get(“classification”) or “”).replace(“Class “, “”).strip()
reason = (rec.get(“reason_for_recall”) or “”).strip()
rdate = (rec.get(“recall_initiation_date”) or rec.get(“report_date”) or “”).strip()
dist = (rec.get(“distribution_pattern”) or “”).strip()
lots = (rec.get(“code_info”) or “”).strip()  # often contains NDC for drugs
status = (rec.get(“status”) or “”).strip()
try:
c.execute(””“INSERT INTO oh_recalls (source, recall_id, brand, product_description, upc,
classification, reason, recall_date, distribution, lot_codes, status, raw_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (recall_id) DO UPDATE SET
status = EXCLUDED.status, fetched_at = CURRENT_TIMESTAMP”””,
(“fda_drug”, rid, brand, desc, “”, cls, reason, rdate, dist, lots, status, json.dumps(rec)))
inserted = inserted + 1
except Exception as e:
skipped = skipped + 1
print(”[fda_drug] skip “ + rid + “: “ + str(e))
conn.commit()
update_ingest_log(“fda_drug”, success=True, record_count=inserted)
return {“source”: “fda_drug”, “fetched”: len(results), “inserted”: inserted, “skipped”: skipped}
except Exception as e:
update_ingest_log(“fda_drug”, success=False, error=str(e))
return {“source”: “fda_drug”, “error”: str(e), “inserted”: inserted}

# ── INGEST: openFDA DEVICE RECALLS ────────────────────────────────────────────

def ingest_openfda_devices(window_days: int = INGEST_WINDOW_DAYS) -> dict:
“”“Pull medical device recalls from openFDA /device/enforcement.json.
Same schema, same upsert pattern as drugs. source=‘fda_device’.”””
cutoff = (datetime.utcnow() - timedelta(days=window_days)).strftime(”%Y%m%d”)
today = datetime.utcnow().strftime(”%Y%m%d”)
search_str = “report_date:[” + cutoff + “+TO+” + today + “]”
full_url = OPENFDA_DEVICE_URL + “?search=” + search_str + “&limit=1000”
if OPENFDA_KEY:
full_url = full_url + “&api_key=” + OPENFDA_KEY
headers = {“User-Agent”: “Mozilla/5.0 (ourhealthwatch/0.1.3)”}
inserted = 0
skipped = 0
try:
r = requests.get(full_url, headers=headers, timeout=30)
if r.status_code != 200:
err = “HTTP “ + str(r.status_code) + “ “ + r.text[:200]
update_ingest_log(“fda_device”, success=False, error=err)
return {“source”: “fda_device”, “error”: err, “inserted”: 0}
data = r.json()
results = data.get(“results”, [])
with get_db() as conn:
c = conn.cursor()
for rec in results:
rid = “fda_device_” + (rec.get(“recall_number”) or rec.get(“event_id”) or “”)
if rid == “fda_device_”:
skipped = skipped + 1
continue
brand = (rec.get(“recalling_firm”) or “”).strip()
desc = (rec.get(“product_description”) or “”).strip()
cls = (rec.get(“classification”) or “”).replace(“Class “, “”).strip()
reason = (rec.get(“reason_for_recall”) or “”).strip()
rdate = (rec.get(“recall_initiation_date”) or rec.get(“report_date”) or “”).strip()
dist = (rec.get(“distribution_pattern”) or “”).strip()
lots = (rec.get(“code_info”) or “”).strip()
status = (rec.get(“status”) or “”).strip()
try:
c.execute(””“INSERT INTO oh_recalls (source, recall_id, brand, product_description, upc,
classification, reason, recall_date, distribution, lot_codes, status, raw_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (recall_id) DO UPDATE SET
status = EXCLUDED.status, fetched_at = CURRENT_TIMESTAMP”””,
(“fda_device”, rid, brand, desc, “”, cls, reason, rdate, dist, lots, status, json.dumps(rec)))
inserted = inserted + 1
except Exception as e:
skipped = skipped + 1
print(”[fda_device] skip “ + rid + “: “ + str(e))
conn.commit()
update_ingest_log(“fda_device”, success=True, record_count=inserted)
return {“source”: “fda_device”, “fetched”: len(results), “inserted”: inserted, “skipped”: skipped}
except Exception as e:
update_ingest_log(“fda_device”, success=False, error=str(e))
return {“source”: “fda_device”, “error”: str(e), “inserted”: inserted}

# ── INGEST: CDC NORS (foodborne / waterborne outbreaks) ───────────────────────

def ingest_nors(limit: int = NORS_FETCH_LIMIT) -> dict:
“”“Pull NORS outbreaks from data.cdc.gov Socrata API (5xkq-dg7x).
NORS publishes annual aggregates per outbreak: year, month, state,
etiology, primary_mode, illnesses, hospitalizations, deaths.
Stored in oh_outbreaks with source=‘cdc_nors’.”””
url = NORS_URL + “?$order=year DESC&$limit=” + str(limit)
headers = {“User-Agent”: “Mozilla/5.0 (ourhealthwatch/0.1.3)”, “Accept”: “application/json”}
inserted = 0
skipped = 0
try:
r = requests.get(url, headers=headers, timeout=30)
if r.status_code != 200:
err = “HTTP “ + str(r.status_code) + “ “ + r.text[:200]
update_ingest_log(“cdc_nors”, success=False, error=err)
return {“source”: “cdc_nors”, “error”: err, “inserted”: 0}
rows = r.json()
with get_db() as conn:
c = conn.cursor()
for rec in rows:
# Socrata returns string year/month; construct a stable outbreak_id
year = (rec.get(“year”) or “”).strip() if isinstance(rec.get(“year”), str) else str(rec.get(“year”) or “”)
month = (rec.get(“month”) or “”).strip() if isinstance(rec.get(“month”), str) else str(rec.get(“month”) or “”)
state = (rec.get(“state”) or rec.get(“primary_state”) or “”).strip()
etiology = (rec.get(“etiology”) or rec.get(“genus”) or “Unknown”).strip()
mode = (rec.get(“primary_mode”) or rec.get(“transmission_mode”) or “”).strip()
setting = (rec.get(“setting”) or “”).strip()
cdc_id = (rec.get(“cdc_id”) or rec.get(“outbreak_id”) or “”).strip()
oid_seed = cdc_id if cdc_id else (year + “-” + month + “-” + state + “-” + etiology)
oid = “cdc_nors_” + oid_seed.replace(” “, “*”)[:120]
if oid == “cdc_nors*”:
skipped = skipped + 1
continue
try:
illnesses = int(rec.get(“illnesses”) or rec.get(“ill_total”) or 0)
except (TypeError, ValueError):
illnesses = 0
title_parts = []
if etiology and etiology != “Unknown”:
title_parts.append(etiology)
if mode:
title_parts.append(”(” + mode + “)”)
if state:
title_parts.append(“in “ + state)
title = “ “.join(title_parts) if title_parts else “NORS outbreak”
report_date = (year + “-” + month.zfill(2) if year and month else year) or “”
summary_bits = []
if setting:
summary_bits.append(“Setting: “ + setting)
summary_bits.append(“Illnesses: “ + str(illnesses))
try:
hosp = int(rec.get(“hospitalizations”) or 0)
if hosp:
summary_bits.append(“Hospitalizations: “ + str(hosp))
except (TypeError, ValueError):
pass
try:
deaths = int(rec.get(“deaths”) or 0)
if deaths:
summary_bits.append(“Deaths: “ + str(deaths))
except (TypeError, ValueError):
pass
summary = “ · “.join(summary_bits)
try:
c.execute(””“INSERT INTO oh_outbreaks (source, outbreak_id, title, agent, location,
country_code, region, ship_name, cruise_line, cases, report_date, report_url, summary, raw_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (outbreak_id) DO UPDATE SET
cases = EXCLUDED.cases, summary = EXCLUDED.summary, fetched_at = CURRENT_TIMESTAMP”””,
(“cdc_nors”, oid, title, etiology, state, “US”, state, None, None,
illnesses, report_date, “https://wwwn.cdc.gov/norsdashboard/”, summary, json.dumps(rec)))
inserted = inserted + 1
except Exception as e:
skipped = skipped + 1
print(”[cdc_nors] skip “ + oid + “: “ + str(e))
conn.commit()
update_ingest_log(“cdc_nors”, success=True, record_count=inserted)
return {“source”: “cdc_nors”, “fetched”: len(rows), “inserted”: inserted, “skipped”: skipped}
except Exception as e:
update_ingest_log(“cdc_nors”, success=False, error=str(e))
return {“source”: “cdc_nors”, “error”: str(e), “inserted”: inserted}

# ── AUTH HELPERS ──────────────────────────────────────────────────────────────

def hash_password(pw: str) -> str:
return bcrypt.hashpw(pw.encode(“utf-8”), bcrypt.gensalt()).decode(“utf-8”)

def verify_password(pw: str, hashed: str) -> bool:
try:
return bcrypt.checkpw(pw.encode(“utf-8”), hashed.encode(“utf-8”))
except Exception:
return False

def make_jwt(user_id: int, email: str) -> str:
payload = {
“user_id”: user_id,
“email”: email,
“exp”: datetime.utcnow() + timedelta(days=JWT_EXPIRY_DAYS),
}
token = jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGO)
if isinstance(token, bytes):
token = token.decode(“utf-8”)
return token

def decode_jwt(token: str) -> dict:
try:
return jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGO])
except Exception:
raise HTTPException(status_code=401, detail=“Invalid or expired token”)

def require_user(authorization: Optional[str] = Header(None)) -> dict:
if not authorization or not authorization.lower().startswith(“bearer “):
raise HTTPException(status_code=401, detail=“Missing bearer token”)
token = authorization[7:].strip()
payload = decode_jwt(token)
return {“id”: payload.get(“user_id”), “email”: payload.get(“email”)}

def require_admin(x_admin_key: Optional[str] = Header(None)):
expected = os.environ.get(“ADMIN_KEY”, “”)
if not expected or x_admin_key != expected:
raise HTTPException(status_code=403, detail=“Admin only”)
return True

# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────

class RegisterIn(BaseModel):
email: EmailStr
password: str

class LoginIn(BaseModel):
email: EmailStr
password: str

class WatchlistAddIn(BaseModel):
kind: Optional[str] = “product”
brand: Optional[str] = None
product_name: Optional[str] = None
upc: Optional[str] = None
keyword: Optional[str] = None
category: Optional[str] = None
monitoring: Optional[bool] = True

# v0.1.7: place-based watchlist models — mirrors EW’s PlaceItem/PlaceUpdate/PlaceResponse

# with region/state/city added so the OHW search UX can scope by geography label

# even when no lat/lng is provided (server geocodes from the labels).

class PlaceItem(BaseModel):
name: Optional[str] = None  # auto-derived from city/state/region if not given
region: Optional[str] = None
country: Optional[str] = None  # v0.1.8: user can pass; geocode fills if missing
state: Optional[str] = None
city: Optional[str] = None
lat: Optional[float] = None
lng: Optional[float] = None
radius_mi: Optional[float] = 50.0
alert_level: Optional[str] = “realtime”  # off | digest | realtime

class PlaceUpdate(BaseModel):
name: Optional[str] = None
region: Optional[str] = None
country: Optional[str] = None
state: Optional[str] = None
city: Optional[str] = None
radius_mi: Optional[float] = None
alert_level: Optional[str] = None
is_archived: Optional[bool] = None  # legacy
in_my_places: Optional[bool] = None  # v0.1.7 canonical

class GeocodeQuery(BaseModel):
query: str

# ── HEALTH / META ─────────────────────────────────────────────────────────────

@app.api_route(”/health”, methods=[“GET”, “HEAD”])
async def health_check():
return {
“status”: “healthy”,
“timestamp”: datetime.utcnow().isoformat(),
“version”: API_VERSION,
“ingest_status”: get_ingest_status(),
}

# ── AUTH ENDPOINTS ────────────────────────────────────────────────────────────

@app.post(”/auth/register”)
async def register(body: RegisterIn):
pw_hash = hash_password(body.password)
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“INSERT INTO oh_users (email, password_hash) VALUES (%s, %s) RETURNING id”,
(body.email, pw_hash))
user_id = c.fetchone()[0]
conn.commit()
token = make_jwt(user_id, body.email)
return {“token”: token, “user_id”: user_id, “email”: body.email}
except psycopg2.errors.UniqueViolation:
raise HTTPException(status_code=409, detail=“Email already registered”)
except Exception as e:
raise HTTPException(status_code=500, detail=“Registration failed: “ + str(e))

@app.post(”/auth/login”)
async def login(body: LoginIn):
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“SELECT id, password_hash FROM oh_users WHERE email = %s”, (body.email,))
row = c.fetchone()
if not row:
raise HTTPException(status_code=401, detail=“Invalid credentials”)
user_id, pw_hash = row
if not verify_password(body.password, pw_hash):
raise HTTPException(status_code=401, detail=“Invalid credentials”)
token = make_jwt(user_id, body.email)
return {“token”: token, “user_id”: user_id, “email”: body.email}
except HTTPException:
raise
except Exception as e:
raise HTTPException(status_code=500, detail=“Login failed: “ + str(e))

@app.get(”/account”)
async def account(user=Depends(require_user)):
return {“user_id”: user[“id”], “email”: user[“email”]}

@app.delete(”/account”)
async def delete_account(user=Depends(require_user)):
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“DELETE FROM oh_notifications WHERE user_id = %s”, (user[“id”],))
c.execute(“DELETE FROM oh_watchlist WHERE user_id = %s”, (user[“id”],))
c.execute(“DELETE FROM oh_places WHERE user_id = %s”, (user[“id”],))
c.execute(“DELETE FROM oh_users WHERE id = %s”, (user[“id”],))
conn.commit()
return {“status”: “deleted”}
except Exception as e:
raise HTTPException(status_code=500, detail=“Delete failed: “ + str(e))

# ── WATCHLIST ENDPOINTS (skeleton — populated as adapters land) ───────────────

@app.get(”/watchlist”)
async def list_watchlist(user=Depends(require_user)):
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
c.execute(””“SELECT id, kind, brand, product_name, upc, keyword, category,
monitoring, has_alert, last_match_id, last_checked, status, created_at
FROM oh_watchlist WHERE user_id = %s ORDER BY created_at DESC”””, (user[“id”],))
rows = c.fetchall()
items = []
for r in rows:
items.append({
“id”: r[“id”],
“kind”: r[“kind”],
“brand”: r[“brand”],
“product_name”: r[“product_name”],
“upc”: r[“upc”],
“keyword”: r[“keyword”],
“category”: r[“category”],
“monitoring”: r[“monitoring”],
“has_alert”: r[“has_alert”],
“last_match_id”: r[“last_match_id”],
“last_checked”: r[“last_checked”].isoformat() if r[“last_checked”] else None,
“status”: r[“status”],
“created_at”: r[“created_at”].isoformat() if r[“created_at”] else None,
})
return {“results”: items}
except Exception as e:
raise HTTPException(status_code=500, detail=“Watchlist fetch failed: “ + str(e))

@app.post(”/watchlist”)
async def add_watchlist(body: WatchlistAddIn, user=Depends(require_user)):
if not (body.brand or body.product_name or body.keyword):
raise HTTPException(status_code=400, detail=“Provide at least one of: brand, product_name, keyword”)
try:
with get_db() as conn:
c = conn.cursor()
c.execute(””“INSERT INTO oh_watchlist (user_id, kind, brand, product_name, upc, keyword, category, monitoring)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id”””,
(user[“id”], body.kind or “product”, body.brand, body.product_name,
body.upc, body.keyword, body.category,
True if body.monitoring is None else bool(body.monitoring)))
new_id = c.fetchone()[0]
conn.commit()
return {“id”: new_id, “status”: “added”}
except Exception as e:
raise HTTPException(status_code=500, detail=“Add failed: “ + str(e))

@app.delete(”/watchlist/{item_id}”)
async def delete_watchlist(item_id: int, user=Depends(require_user)):
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“DELETE FROM oh_notifications WHERE watchlist_id = %s AND user_id = %s”,
(item_id, user[“id”]))
c.execute(“DELETE FROM oh_watchlist WHERE id = %s AND user_id = %s”,
(item_id, user[“id”]))
conn.commit()
return {“status”: “deleted”}
except Exception as e:
raise HTTPException(status_code=500, detail=“Delete failed: “ + str(e))

def find_best_recall_for_watch(conn, brand: Optional[str], product_name: Optional[str],
upc: Optional[str], keyword: Optional[str]):
“”“Return best matching oh_recalls row for a watchlist entry, or None.
Forked from MW pattern: UPC exact → brand/keyword fuzzy with scoring.
Threshold of 30 keeps it from surfacing weak matches.”””
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
# UPC exact match (when both have it)
if upc:
c.execute(“SELECT * FROM oh_recalls WHERE upc = %s AND status = ‘Ongoing’ LIMIT 1”, (upc,))
row = c.fetchone()
if row:
return dict(row)
# Brand or keyword fuzzy
primary = brand or keyword
if primary:
like = “%” + primary + “%”
c.execute(””“SELECT * FROM oh_recalls
WHERE (brand ILIKE %s OR product_description ILIKE %s)
AND status = ‘Ongoing’
ORDER BY recall_date DESC NULLS LAST LIMIT 50”””, (like, like))
rows = c.fetchall()
best = None
best_score = 0
for row in rows:
d = dict(row)
s = 0
b = (d.get(“brand”) or “”).lower()
desc = (d.get(“product_description”) or “”).lower()
p = primary.lower()
pn = (product_name or “”).lower()
if p and p in b:
s = s + (50 if b == p else 30)
elif p and p in desc:
s = s + 25
if pn and pn in desc:
s = s + 20
elif pn and pn in b:
s = s + 10
if (d.get(“classification”) or “”).startswith(“I”):
s = s + 5
# require primary term to actually match — don’t surface unrelated recalls
if p and p not in b and p not in desc:
continue
if s > best_score:
best_score = s
best = d
if best and best_score >= 30:
return best
return None

def serialize_recall(d) -> Optional[dict]:
if not d:
return None
out = {}
for k, v in d.items():
if isinstance(v, datetime):
out[k] = v.isoformat()
else:
out[k] = v
return out

def find_best_outbreak_for_watch(conn, keyword: Optional[str], category: Optional[str]):
“”“Return best matching oh_outbreaks row for a watchlist entry, or None.
Matches keyword against agent + title + summary (substring). Optionally
scopes to a specific source via category (e.g. ‘cdc_nors’).”””
if not keyword:
return None
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
like = “%” + keyword + “%”
where = [”(agent ILIKE %s OR title ILIKE %s OR summary ILIKE %s)”]
params: list = [like, like, like]
if category:
where.append(“source = %s”)
params.append(category)
sql = (“SELECT * FROM oh_outbreaks WHERE “ + “ AND “.join(where) +
“ ORDER BY report_date DESC NULLS LAST, fetched_at DESC LIMIT 1”)
c.execute(sql, tuple(params))
row = c.fetchone()
return dict(row) if row else None

def *is_outbreak_kind(kind: Optional[str], category: Optional[str]) -> bool:
“”“Classify a watchlist row as outbreak-shaped vs recall-shaped.
Outbreak if kind==‘outbreak’ OR category starts with cdc*/who_.”””
if kind == “outbreak”:
return True
if category and (category.startswith(“cdc_”) or category.startswith(“who_”)):
return True
return False

@app.get(”/watchlist/{item_id}/refresh”)
async def refresh_watchlist_item(item_id: int, user=Depends(require_user)):
“”“On-demand recheck for a single watchlist item — used by the drawer.
Type-aware: routes to oh_outbreaks search for outbreak-shaped items,
oh_recalls search for recall-shaped items. Returns {type, matched,
recall|outbreak, last_checked}.”””
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
c.execute(””“SELECT id, brand, product_name, upc, keyword, kind, category
FROM oh_watchlist WHERE id = %s AND user_id = %s AND status = ‘active’”””,
(item_id, user[“id”]))
wrow = c.fetchone()
if not wrow:
raise HTTPException(status_code=404, detail=“Watchlist item not found”)

```
        now = datetime.utcnow()
        is_outbreak = _is_outbreak_kind(wrow["kind"], wrow["category"])

        if is_outbreak:
            best = find_best_outbreak_for_watch(conn, wrow["keyword"], wrow["category"])
            has_alert = best is not None
            last_match_id = best["outbreak_id"] if best else None
            payload_type = "outbreak"
            outbreak_payload = serialize_recall(best)  # serializer is type-agnostic
            recall_payload = None
        else:
            best = find_best_recall_for_watch(conn, wrow["brand"], wrow["product_name"],
                                               wrow["upc"], wrow["keyword"])
            has_alert = best is not None
            last_match_id = best["recall_id"] if best else None
            payload_type = "recall"
            recall_payload = serialize_recall(best)
            outbreak_payload = None

        uc = conn.cursor()
        uc.execute("""UPDATE oh_watchlist
            SET has_alert = %s, last_match_id = %s, last_checked = %s
            WHERE id = %s""", (has_alert, last_match_id, now, item_id))
        conn.commit()

        return {
            "id": item_id,
            "type": payload_type,
            "has_alert": has_alert,
            "matched": has_alert,
            "recall": recall_payload,
            "outbreak": outbreak_payload,
            "last_checked": now.isoformat()
        }
except HTTPException:
    raise
except Exception as e:
    raise HTTPException(status_code=500, detail="Refresh failed: " + str(e))
```

# ──────────────────────────────────────────────────────────────────────────────

# v0.1.7: PLACE-BASED WATCHLIST (mirrors EW pattern)

# ──────────────────────────────────────────────────────────────────────────────

# 

# Watchlist primitive = a PLACE (region + optional state/city, geocoded to

# lat/lng). User searches by location, place lands in Watchlist, cron monitors

# the location for outbreaks + recalls relevant to it, alerts fire on match.

# 

# Save to My Places flips in_my_places=TRUE on the same row — the place stays

# in Watchlist AND now appears in My Places. Identical semantics to EW v0.1.6+.

# ──────────────────────────────────────────────────────────────────────────────

def _geocode_text(text: str) -> Optional[dict]:
“”“Forward-geocode a free-text query via Google. Returns
{lat, lng, formatted_address, country, country_code, state, city} or None.

```
v0.1.8: also parses address_components so we can match against each source's
native location field — FDA recalls match on state (free text), NORS matches
on state (region column), WHO DON will match on country_code. EarthWatch
only needs lat/lng (spatial join); OHW needs the parsed labels because
its sources don't publish coordinates."""
if not GOOGLE_GEOCODING_API_KEY or not text or len(text.strip()) < 2:
    return None
try:
    url = ("https://maps.googleapis.com/maps/api/geocode/json?address="
           + urllib.parse.quote(text.strip())
           + "&key=" + GOOGLE_GEOCODING_API_KEY)
    req = urllib.request.Request(url, headers={"User-Agent": "OurHealthWatch/0.1"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
except Exception as e:
    print("[_geocode_text] " + str(e))
    return None
if data.get("status") != "OK":
    return None
results = data.get("results") or []
if not results:
    return None
top = results[0]
loc = ((top.get("geometry") or {}).get("location")) or {}
lat = loc.get("lat")
lng = loc.get("lng")
if lat is None or lng is None:
    return None
# Parse address_components into our canonical fields. Google returns each
# component with a `types` array (e.g. ["country", "political"]); we walk
# them and pick the one matching each level we care about.
country = None
country_code = None
state = None
city = None
for comp in (top.get("address_components") or []):
    types = comp.get("types") or []
    if "country" in types:
        country = comp.get("long_name")
        country_code = comp.get("short_name")  # ISO 3166-1 alpha-2
    elif "administrative_area_level_1" in types:
        state = comp.get("long_name")
    elif "locality" in types:
        city = comp.get("long_name")
    elif "postal_town" in types and not city:
        # UK-style fallback when locality isn't present
        city = comp.get("long_name")
return {
    "lat": float(lat),
    "lng": float(lng),
    "formatted_address": top.get("formatted_address", ""),
    "country": country,
    "country_code": country_code,
    "state": state,
    "city": city,
}
```

@app.post(”/geocode”)
async def geocode_place(q: GeocodeQuery, user=Depends(require_user)):
“”“Forward geocode a free-text query (city, address, landmark) via Google.
Returns up to 5 candidates so the user can pick the right match.
Backend-side so the API key never lives in the frontend.
Ported verbatim from EarthWatch /ew/geocode.”””
if not GOOGLE_GEOCODING_API_KEY:
raise HTTPException(status_code=503, detail=“Geocoding service not configured”)
text = (q.query or “”).strip()
if len(text) < 2:
raise HTTPException(status_code=400, detail=“Query too short”)
try:
url = (“https://maps.googleapis.com/maps/api/geocode/json?address=”
+ urllib.parse.quote(text)
+ “&key=” + GOOGLE_GEOCODING_API_KEY)
req = urllib.request.Request(url, headers={“User-Agent”: “OurHealthWatch/0.1”})
with urllib.request.urlopen(req, timeout=10) as resp:
data = json.loads(resp.read().decode(“utf-8”, errors=“replace”))
except Exception as e:
print(”[geocode] “ + str(e))
raise HTTPException(status_code=502, detail=“Geocoding lookup failed”)
status = data.get(“status”)
if status == “ZERO_RESULTS”:
return {“candidates”: []}
if status != “OK”:
print(”[geocode] Google returned status=” + str(status) + “ for query: “ + text)
raise HTTPException(status_code=502, detail=“Geocoding lookup failed”)
candidates = []
for r in (data.get(“results”) or [])[:5]:
loc = ((r.get(“geometry”) or {}).get(“location”)) or {}
lat = loc.get(“lat”)
lng = loc.get(“lng”)
if lat is None or lng is None:
continue
candidates.append({
“formatted_address”: r.get(“formatted_address”, “”),
“lat”: float(lat),
“lng”: float(lng),
“place_id”: r.get(“place_id”, “”),
})
return {“candidates”: candidates}

def _derive_place_name(city: Optional[str], state: Optional[str], region: Optional[str]) -> str:
“”“Auto-derive a display name from city/state/region when the caller doesn’t
supply one. Prefer the most specific label available.”””
parts = []
if city:
parts.append(city.strip())
if state:
parts.append(state.strip())
if not parts and region:
parts.append(region.strip())
if not parts:
return “Unnamed place”
return “, “.join(parts)

@app.get(”/places”)
async def list_places(user=Depends(require_user)):
“”“All places this user has on their Watchlist. Frontend filters
in_my_places=TRUE client-side for the My Places view.”””
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
c.execute(””“SELECT id, name, region, country, country_code, state, city,
formatted_address, lat, lng, radius_mi,
alert_level, is_archived, in_my_places, created_at
FROM oh_places WHERE user_id = %s ORDER BY created_at DESC”””, (user[“id”],))
rows = c.fetchall()
items = []
for r in rows:
items.append({
“id”: r[“id”],
“name”: r[“name”],
“region”: r[“region”],
“country”: r[“country”],
“country_code”: r[“country_code”],
“state”: r[“state”],
“city”: r[“city”],
“formatted_address”: r[“formatted_address”],
“lat”: float(r[“lat”]),
“lng”: float(r[“lng”]),
“radius_mi”: float(r[“radius_mi”]),
“alert_level”: r[“alert_level”],
“is_archived”: bool(r[“is_archived”]),
“in_my_places”: bool(r[“in_my_places”]),
“created_at”: r[“created_at”].isoformat() if r[“created_at”] else None,
})
return {“results”: items}
except Exception as e:
raise HTTPException(status_code=500, detail=“Places fetch failed: “ + str(e))

@app.post(”/places”)
async def add_place(body: PlaceItem, user=Depends(require_user)):
“”“Add a place to the Watchlist. Cron will start monitoring it on the
next tick. lat/lng auto-derived from city/state/region if not supplied
(requires GOOGLE_GEOCODING_API_KEY).

```
v0.1.8: also resolves country / country_code / formatted_address from the
geocode and persists them, so per-source match (FDA state, NORS state,
WHO DON country_code) can run without re-geocoding."""
lat = body.lat
lng = body.lng
# v0.1.8: geocoded fields. Prefer user-supplied; fall back to parsed geocode.
country = body.country
country_code = None
geo_state = body.state
geo_city = body.city
formatted_address = None
# Auto-geocode if coords missing
if lat is None or lng is None:
    seed = body.city or body.state or body.country or body.region
    if not seed:
        raise HTTPException(status_code=400, detail="Provide either lat/lng or city/state/country/region to geocode")
    # Build the richest query the labels allow
    parts = [p for p in [body.city, body.state, body.country, body.region] if p]
    query = ", ".join(parts)
    geo = _geocode_text(query)
    if not geo:
        raise HTTPException(status_code=400, detail="Could not geocode location: " + query)
    lat = geo["lat"]
    lng = geo["lng"]
    # Prefer parsed values over user input — Google's canonical forms match
    # source feeds better than free typing (e.g. "MX" vs "Mexico" vs "mex").
    country = geo.get("country") or country
    country_code = geo.get("country_code")
    geo_state = geo.get("state") or geo_state
    geo_city = geo.get("city") or geo_city
    formatted_address = geo.get("formatted_address")
if lat < -90 or lat > 90:
    raise HTTPException(status_code=400, detail="lat must be between -90 and 90")
if lng < -180 or lng > 180:
    raise HTTPException(status_code=400, detail="lng must be between -180 and 180")
radius = float(body.radius_mi) if body.radius_mi is not None else 50.0
if radius <= 0 or radius > 1000:
    raise HTTPException(status_code=400, detail="radius_mi must be between 0 and 1000")
alert_level = body.alert_level or "realtime"
if alert_level not in ("off", "digest", "realtime"):
    alert_level = "realtime"
name = (body.name or _derive_place_name(geo_city, geo_state, body.region)).strip()
try:
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""INSERT INTO oh_places
            (user_id, name, region, country, country_code, state, city,
             formatted_address, lat, lng, radius_mi, alert_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at""",
            (user["id"], name, body.region, country, country_code,
             geo_state, geo_city, formatted_address,
             lat, lng, radius, alert_level))
        row = c.fetchone()
        conn.commit()
    return {
        "id": row[0],
        "name": name,
        "region": body.region,
        "country": country,
        "country_code": country_code,
        "state": geo_state,
        "city": geo_city,
        "formatted_address": formatted_address,
        "lat": lat,
        "lng": lng,
        "radius_mi": radius,
        "alert_level": alert_level,
        "is_archived": False,
        "in_my_places": False,
        "created_at": row[1].isoformat() if row[1] else None,
    }
except Exception as e:
    raise HTTPException(status_code=500, detail="Add place failed: " + str(e))
```

@app.patch(”/places/{place_id}”)
async def update_place(place_id: int, body: PlaceUpdate, user=Depends(require_user)):
“”“Update a place. Most common use: toggle in_my_places to save/unsave.”””
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“SELECT id FROM oh_places WHERE id = %s AND user_id = %s”,
(place_id, user[“id”]))
if not c.fetchone():
raise HTTPException(status_code=404, detail=“Place not found”)
sets = []
params: list = []
if body.name is not None:
sets.append(“name = %s”)
params.append(body.name)
if body.region is not None:
sets.append(“region = %s”)
params.append(body.region)
if body.country is not None:
sets.append(“country = %s”)
params.append(body.country)
if body.state is not None:
sets.append(“state = %s”)
params.append(body.state)
if body.city is not None:
sets.append(“city = %s”)
params.append(body.city)
if body.radius_mi is not None:
if body.radius_mi <= 0 or body.radius_mi > 1000:
raise HTTPException(status_code=400, detail=“radius_mi must be between 0 and 1000”)
sets.append(“radius_mi = %s”)
params.append(body.radius_mi)
if body.alert_level is not None:
if body.alert_level not in (“off”, “digest”, “realtime”):
raise HTTPException(status_code=400, detail=“alert_level must be off/digest/realtime”)
sets.append(“alert_level = %s”)
params.append(body.alert_level)
if body.is_archived is not None:
sets.append(“is_archived = %s”)
params.append(bool(body.is_archived))
if body.in_my_places is not None:
sets.append(“in_my_places = %s”)
params.append(bool(body.in_my_places))
if not sets:
return {“status”: “no_changes”, “id”: place_id}
params.append(place_id)
params.append(user[“id”])
c.execute(“UPDATE oh_places SET “ + “, “.join(sets) +
“ WHERE id = %s AND user_id = %s”, tuple(params))
conn.commit()
return {“status”: “updated”, “id”: place_id}
except HTTPException:
raise
except Exception as e:
raise HTTPException(status_code=500, detail=“Update place failed: “ + str(e))

@app.delete(”/places/{place_id}”)
async def delete_place(place_id: int, user=Depends(require_user)):
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“DELETE FROM oh_places WHERE id = %s AND user_id = %s”,
(place_id, user[“id”]))
conn.commit()
if c.rowcount == 0:
raise HTTPException(status_code=404, detail=“Place not found”)
return {“status”: “deleted”}
except HTTPException:
raise
except Exception as e:
raise HTTPException(status_code=500, detail=“Delete place failed: “ + str(e))

@app.get(”/places/{place_id}/events”)
async def get_place_events(place_id: int, user=Depends(require_user)):
“”“Drawer payload — what’s currently happening at this place.

```
v0.1.8: PER-SOURCE match strategy. Each source has a different native
location field, so the WHERE clause differs by source:

  - fda_drug / fda_device: match place.state against recalls.distribution
    (FDA's distribution_pattern is free text like "Nationwide" or
    "CA, TX, FL"). Nationwide rolls up to everyone.
  - cdc_nors: match place.state against outbreaks.region (NORS region
    column holds the US state). country_code is always 'US' for NORS.
  - who_don: match place.country_code against outbreaks.country_code
    OR place.country against outbreaks.region (WHO publishes by country).
    (Adapter ships v0.1.x+; the match logic is in place now so it just
    lights up when WHO DON rows land in oh_outbreaks.)
  - cdc_vsp: ship-based, no place match. Skipped entirely.

The geocoded country/country_code/state on oh_places are populated by
POST /places (which parses Google's address_components). EW does pure
spatial join via PostGIS — OHW can't because FDA/NORS/WHO publish
administrative labels, not coordinates."""
try:
    with get_db() as conn:
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("""SELECT id, name, region, country, country_code, state, city,
            formatted_address, lat, lng, radius_mi,
            in_my_places, is_archived
            FROM oh_places WHERE id = %s AND user_id = %s""",
            (place_id, user["id"]))
        place = c.fetchone()
        if not place:
            raise HTTPException(status_code=404, detail="Place not found")

        state = (place["state"] or "").strip()
        city = (place["city"] or "").strip()
        region = (place["region"] or "").strip()
        country = (place["country"] or "").strip()
        country_code = (place["country_code"] or "").strip()

        # ── Outbreak match: per-source UNION ALL ────────────────────────
        # Each branch only fires when the matching place fields exist.
        outbreaks: list = []
        seen_oids = set()

        # NORS branch: state-level, US-only.
        if state:
            like = "%" + state + "%"
            c.execute("""SELECT id, source, outbreak_id, title, agent, location,
                country_code, region, cases, report_date, report_url, summary, fetched_at
                FROM oh_outbreaks
                WHERE source = 'cdc_nors'
                  AND (region ILIKE %s OR location ILIKE %s)
                ORDER BY report_date DESC NULLS LAST, fetched_at DESC LIMIT 25""",
                (like, like))
            for r in c.fetchall():
                if r["outbreak_id"] in seen_oids:
                    continue
                seen_oids.add(r["outbreak_id"])
                outbreaks.append(_serialize_outbreak_row(r))

        # WHO DON branch: country-level, global.
        # Lights up when WHO DON adapter ships and writes rows with
        # country_code populated. Until then, this branch returns 0 rows.
        if country_code or country:
            where = []
            params: list = []
            if country_code:
                where.append("country_code ILIKE %s")
                params.append(country_code)
            if country:
                where.append("region ILIKE %s")
                params.append("%" + country + "%")
            sql_who = ("""SELECT id, source, outbreak_id, title, agent, location,
                country_code, region, cases, report_date, report_url, summary, fetched_at
                FROM oh_outbreaks
                WHERE source = 'who_don'
                  AND (""" + " OR ".join(where) + """)
                ORDER BY report_date DESC NULLS LAST, fetched_at DESC LIMIT 25""")
            c.execute(sql_who, tuple(params))
            for r in c.fetchall():
                if r["outbreak_id"] in seen_oids:
                    continue
                seen_oids.add(r["outbreak_id"])
                outbreaks.append(_serialize_outbreak_row(r))

        # Fallback branch: any source we don't have explicit logic for yet
        # (future adapters), best-effort string match on city/state/country.
        fallback_where = []
        fallback_params: list = []
        if city:
            fallback_where.append("(location ILIKE %s OR summary ILIKE %s)")
            fallback_params.append("%" + city + "%")
            fallback_params.append("%" + city + "%")
        if state:
            fallback_where.append("(region ILIKE %s OR location ILIKE %s)")
            fallback_params.append("%" + state + "%")
            fallback_params.append("%" + state + "%")
        if country_code:
            fallback_where.append("country_code ILIKE %s")
            fallback_params.append(country_code)
        if fallback_where:
            sql_fb = ("""SELECT id, source, outbreak_id, title, agent, location,
                country_code, region, cases, report_date, report_url, summary, fetched_at
                FROM oh_outbreaks
                WHERE source NOT IN ('cdc_nors', 'who_don', 'cdc_vsp')
                  AND (""" + " OR ".join(fallback_where) + """)
                ORDER BY report_date DESC NULLS LAST, fetched_at DESC LIMIT 25""")
            c.execute(sql_fb, tuple(fallback_params))
            for r in c.fetchall():
                if r["outbreak_id"] in seen_oids:
                    continue
                seen_oids.add(r["outbreak_id"])
                outbreaks.append(_serialize_outbreak_row(r))

        # ── Recall match: FDA distribution_pattern is free text ────────
        # State match + Nationwide rollup. FDA recalls are US-only by
        # source design — no country/region branch.
        recalls: list = []
        if state:
            like = "%" + state + "%"
            c.execute("""SELECT id, source, recall_id, brand, product_description,
                upc, classification, reason, recall_date, distribution, lot_codes,
                status, fetched_at FROM oh_recalls
                WHERE source IN ('fda_drug', 'fda_device')
                  AND (distribution ILIKE %s OR distribution ILIKE 'Nationwide%%')
                ORDER BY recall_date DESC NULLS LAST, fetched_at DESC LIMIT 25""",
                (like,))
            for r in c.fetchall():
                recalls.append({
                    "id": r["id"],
                    "source": r["source"],
                    "recall_id": r["recall_id"],
                    "brand": r["brand"],
                    "product_description": r["product_description"],
                    "upc": r["upc"],
                    "classification": r["classification"],
                    "reason": r["reason"],
                    "recall_date": r["recall_date"],
                    "distribution": r["distribution"],
                    "lot_codes": r["lot_codes"],
                    "status": r["status"],
                    "fetched_at": r["fetched_at"].isoformat() if r["fetched_at"] else None,
                })

        return {
            "place": {
                "id": place["id"],
                "name": place["name"],
                "region": place["region"],
                "country": place["country"],
                "country_code": place["country_code"],
                "state": place["state"],
                "city": place["city"],
                "formatted_address": place["formatted_address"],
                "lat": float(place["lat"]),
                "lng": float(place["lng"]),
                "radius_mi": float(place["radius_mi"]),
                "in_my_places": bool(place["in_my_places"]),
                "is_archived": bool(place["is_archived"]),
            },
            "outbreaks": outbreaks,
            "recalls": recalls,
            "outbreak_count": len(outbreaks),
            "recall_count": len(recalls),
            "checked_at": datetime.utcnow().isoformat(),
        }
except HTTPException:
    raise
except Exception as e:
    raise HTTPException(status_code=500, detail="Place events fetch failed: " + str(e))
```

def _serialize_outbreak_row(r) -> dict:
“”“Shared serializer for per-source outbreak branches in /places/{id}/events.”””
return {
“id”: r[“id”],
“source”: r[“source”],
“outbreak_id”: r[“outbreak_id”],
“title”: r[“title”],
“agent”: r[“agent”],
“location”: r[“location”],
“country_code”: r[“country_code”],
“region”: r[“region”],
“cases”: r[“cases”],
“report_date”: r[“report_date”],
“report_url”: r[“report_url”],
“summary”: r[“summary”],
“fetched_at”: r[“fetched_at”].isoformat() if r[“fetched_at”] else None,
}

@app.get(”/regions”)
async def list_regions():
“”“Return the 12 canonical regions used by the Search UI. Public — no auth.”””
return {“regions”: OH_REGIONS}

# ── RECALLS ENDPOINTS (firehose + opt-in pattern) ─────────────────────────────

@app.get(”/recalls/recent”)
async def recent_recalls(
source: Optional[str] = None,
limit: int = 25,
status: str = “Ongoing”,
include_all_status: bool = False,
):
“”“The ‘show all’ firehose. Default: last 30 days, Ongoing status only,
limit 25. Frontend renders this with an opt-in ‘Add to Watchlist’ button
on each card. include_all_status=true bypasses the Ongoing filter.”””
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
where = [“1=1”]
params: list = []
if source:
where.append(“source = %s”)
params.append(source)
else:
where.append(“source LIKE ‘fda_%%’”)
if not include_all_status:
where.append(“status = %s”)
params.append(status)
params.append(int(limit))
sql = (“SELECT id, source, recall_id, brand, product_description, upc, “
“classification, reason, recall_date, distribution, lot_codes, status, fetched_at “
“FROM oh_recalls WHERE “ + “ AND “.join(where) +
“ ORDER BY recall_date DESC NULLS LAST, fetched_at DESC LIMIT %s”)
c.execute(sql, tuple(params))
rows = c.fetchall()
items = []
for r in rows:
items.append({
“id”: r[“id”],
“source”: r[“source”],
“recall_id”: r[“recall_id”],
“brand”: r[“brand”],
“product_description”: r[“product_description”],
“upc”: r[“upc”],
“classification”: r[“classification”],
“reason”: r[“reason”],
“recall_date”: r[“recall_date”],
“distribution”: r[“distribution”],
“lot_codes”: r[“lot_codes”],
“status”: r[“status”],
“fetched_at”: r[“fetched_at”].isoformat() if r[“fetched_at”] else None,
})
# Include per-source last_checked timestamps so the UI can render
# “Last checked X min ago” even on an empty list. Source filter, if
# passed, scopes the timestamp to just that source.
ingest = get_ingest_status()
if source and source in ingest:
checked = {source: ingest[source]}
else:
checked = {k: v for k, v in ingest.items() if k.startswith(“fda_”)}
return {“results”: items, “ingest_status”: checked, “count”: len(items)}
except Exception as e:
raise HTTPException(status_code=500, detail=“Recent recalls fetch failed: “ + str(e))

# ── OUTBREAKS ENDPOINTS (firehose + opt-in pattern) ───────────────────────────

@app.get(”/outbreaks/recent”)
async def recent_outbreaks(
source: Optional[str] = None,
limit: int = 25,
agent: Optional[str] = None,
region: Optional[str] = None,
):
“”“Outbreak firehose. Source can be ‘cdc_nors’, ‘who_don’, ‘cdc_vsp’, etc.
Optional filters by agent (etiology) and region (state/country).
Adapters land progressively: NORS in v0.1.3, WHO DON in v0.1.5, VSP later.”””
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
where = [“1=1”]
params: list = []
if source:
where.append(“source = %s”)
params.append(source)
if agent:
where.append(“agent ILIKE %s”)
params.append(”%” + agent + “%”)
if region:
where.append(“region ILIKE %s”)
params.append(”%” + region + “%”)
params.append(int(limit))
sql = (“SELECT id, source, outbreak_id, title, agent, location, country_code, region, “
“ship_name, cruise_line, cases, report_date, report_url, summary, fetched_at “
“FROM oh_outbreaks WHERE “ + “ AND “.join(where) +
“ ORDER BY report_date DESC NULLS LAST, fetched_at DESC LIMIT %s”)
c.execute(sql, tuple(params))
rows = c.fetchall()
items = []
for r in rows:
items.append({
“id”: r[“id”],
“source”: r[“source”],
“outbreak_id”: r[“outbreak_id”],
“title”: r[“title”],
“agent”: r[“agent”],
“location”: r[“location”],
“country_code”: r[“country_code”],
“region”: r[“region”],
“ship_name”: r[“ship_name”],
“cruise_line”: r[“cruise_line”],
“cases”: r[“cases”],
“report_date”: r[“report_date”],
“report_url”: r[“report_url”],
“summary”: r[“summary”],
“fetched_at”: r[“fetched_at”].isoformat() if r[“fetched_at”] else None,
})
ingest = get_ingest_status()
if source and source in ingest:
checked = {source: ingest[source]}
else:
checked = {k: v for k, v in ingest.items()
if k.startswith(“cdc_”) or k.startswith(“who_”)}
return {“results”: items, “ingest_status”: checked, “count”: len(items)}
except Exception as e:
raise HTTPException(status_code=500, detail=“Recent outbreaks fetch failed: “ + str(e))

# ── NOTIFICATIONS ENDPOINTS ───────────────────────────────────────────────────

@app.get(”/notifications”)
async def list_notifications(user=Depends(require_user)):
try:
with get_db() as conn:
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
c.execute(””“SELECT id, watchlist_id, message, source, source_ref_id, email_sent, created_at
FROM oh_notifications WHERE user_id = %s ORDER BY created_at DESC LIMIT 100”””,
(user[“id”],))
rows = c.fetchall()
items = []
for r in rows:
items.append({
“id”: r[“id”],
“watchlist_id”: r[“watchlist_id”],
“message”: r[“message”],
“source”: r[“source”],
“source_ref_id”: r[“source_ref_id”],
“email_sent”: r[“email_sent”],
“created_at”: r[“created_at”].isoformat() if r[“created_at”] else None,
})
return {“results”: items}
except Exception as e:
raise HTTPException(status_code=500, detail=“Notifications fetch failed: “ + str(e))

@app.delete(”/notifications/{notif_id}”)
async def delete_notification(notif_id: int, user=Depends(require_user)):
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“DELETE FROM oh_notifications WHERE id = %s AND user_id = %s”,
(notif_id, user[“id”]))
conn.commit()
return {“status”: “deleted”}
except Exception as e:
raise HTTPException(status_code=500, detail=“Delete failed: “ + str(e))

# ── ADMIN ENDPOINTS ───────────────────────────────────────────────────────────

@app.get(”/admin/signup-stats”)
async def admin_signup_stats(x_admin_token: str = Header(None, alias=“X-Admin-Token”)):
# Uses ADMIN_STATS_KEY (separate from ADMIN_KEY) so read-only stats access
# can be shared with partners/team without granting operational control.
expected = os.environ.get(“ADMIN_STATS_KEY”, “”)
if not expected or x_admin_token != expected:
raise HTTPException(status_code=403, detail=“Admin only”)
try:
with get_db() as conn:
c = conn.cursor()
c.execute(“SELECT COUNT(*) FROM oh_users”)
total = c.fetchone()[0]
c.execute(“SELECT COUNT(*) FROM oh_users WHERE created_at >= NOW() - INTERVAL ‘7 days’”)
week = c.fetchone()[0]
c.execute(“SELECT COUNT(*) FROM oh_users WHERE created_at >= NOW() - INTERVAL ‘24 hours’”)
day = c.fetchone()[0]
c.execute(“SELECT COUNT(*) FROM oh_watchlist”)
watch = c.fetchone()[0]
c.execute(“SELECT COUNT(*) FROM oh_places”)
places = c.fetchone()[0]
return {
“users_total”: total,
“users_last_7d”: week,
“users_last_24h”: day,
“watchlist_items_total”: watch,
“places_total”: places,
“version”: API_VERSION,
}
except Exception as e:
raise HTTPException(status_code=500, detail=“Stats failed: “ + str(e))

@app.get(”/admin/ingest-status”)
async def admin_ingest_status(_admin=Depends(require_admin)):
“”“Inspect per-source ingest health. Useful when an adapter goes quiet.”””
return {“ingest_status”: get_ingest_status(), “checked_at”: datetime.utcnow().isoformat()}

@app.post(”/admin/refresh-recalls”)
async def admin_refresh_recalls(_admin=Depends(require_admin)):
“”“Manual trigger for openFDA recall ingests (drug + device). Useful for
forcing a refresh between cron ticks or smoke-testing.”””
drug_res = ingest_openfda_drugs()
device_res = ingest_openfda_devices()
return {“drug”: drug_res, “device”: device_res, “ran_at”: datetime.utcnow().isoformat()}

@app.post(”/admin/refresh-outbreaks”)
async def admin_refresh_outbreaks(_admin=Depends(require_admin)):
“”“Manual trigger for outbreak ingests (NORS + future WHO DON, VSP).
Returns per-source result dicts.”””
nors_res = ingest_nors()
return {“nors”: nors_res, “ran_at”: datetime.utcnow().isoformat()}

# ── CRON ──────────────────────────────────────────────────────────────────────

def run_watchlist_check():
“”“Background tick. v0.1.3 runs openFDA drug+device recall ingest plus
CDC NORS foodborne/waterborne outbreaks. Future adapters (WHO DON,
State Dept, VSP) land progressively. Each adapter calls update_ingest_log()
on completion, success or failure.”””
print(”[cron] tick at “ + datetime.utcnow().isoformat())
try:
drug_res = ingest_openfda_drugs()
print(”[cron] fda_drug: “ + json.dumps(drug_res))
except Exception as e:
print(”[cron] fda_drug exception: “ + str(e))
update_ingest_log(“fda_drug”, success=False, error=str(e))
try:
device_res = ingest_openfda_devices()
print(”[cron] fda_device: “ + json.dumps(device_res))
except Exception as e:
print(”[cron] fda_device exception: “ + str(e))
update_ingest_log(“fda_device”, success=False, error=str(e))
try:
nors_res = ingest_nors()
print(”[cron] cdc_nors: “ + json.dumps(nors_res))
except Exception as e:
print(”[cron] cdc_nors exception: “ + str(e))
update_ingest_log(“cdc_nors”, success=False, error=str(e))
update_ingest_log(“system”, success=True, record_count=0)

def run_scheduler():
schedule.every(WATCHLIST_CHECK_INTERVAL_HOURS).hours.do(run_watchlist_check)
# Run an initial ingest on startup so /recalls/recent has fresh data
# immediately rather than waiting up to 12 hrs for the first cron tick.
try:
run_watchlist_check()
except Exception as e:
print(”[scheduler] startup ingest failed: “ + str(e))
while True:
schedule.run_pending()
time_mod.sleep(60)

# ── STARTUP ───────────────────────────────────────────────────────────────────

@app.on_event(“startup”)
async def startup_event():
init_db()
# Seed system row in ingest_log so /health has something to show on day one.
update_ingest_log(“system”, success=True, record_count=0)
t = threading.Thread(target=run_scheduler, daemon=True)
t.start()
print(“OurHealth.Watch API v” + API_VERSION + “ started (cron thread up)”)
