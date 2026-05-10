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
from contextlib import contextmanager
from typing import Optional, List

# ── ENV VARS (set in Render) ──────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable not set. "
        "In Render, link a Postgres database to this service or set DATABASE_URL manually."
    )
# Render sometimes uses postgres:// (legacy). psycopg2 accepts both, but normalize for safety.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

SECRET_KEY = os.environ.get("SECRET_KEY", os.environ.get("JWT_SECRET", "ohw-fallback-change-me"))
OPENFDA_KEY = os.environ.get("OPENFDA_KEY", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "alerts@ourhealth.watch")

API_VERSION = "0.1.4"
JWT_ALGO = "HS256"
JWT_EXPIRY_DAYS = 7
WATCHLIST_CHECK_INTERVAL_HOURS = 12  # free tier; premium will be 1hr
INGEST_WINDOW_DAYS = 30  # rolling window for openFDA drug+device recalls
OPENFDA_DRUG_URL = "https://api.fda.gov/drug/enforcement.json"
OPENFDA_DEVICE_URL = "https://api.fda.gov/device/enforcement.json"
NORS_URL = "https://data.cdc.gov/resource/5xkq-dg7x.json"  # CDC NORS foodborne/waterborne outbreaks
NORS_FETCH_LIMIT = 200

app = FastAPI(title="OurHealth.Watch API", version=API_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

            # pg_trgm enables fuzzy similarity for future /suggest endpoint
            try:
                c.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            except Exception as ex:
                print("[init_db] pg_trgm note: " + str(ex))

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

            conn.commit()
            print("[init_db] tables ready (oh_users, oh_watchlist, oh_notifications, oh_recalls, oh_outbreaks, oh_ingest_log)")
    except Exception as e:
        print("[init_db] WARNING: " + str(e))


def update_ingest_log(source: str, success: bool, record_count: int = 0, error: Optional[str] = None):
    """Update per-source ingest log. Called by every adapter on every run."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            now = datetime.utcnow()
            if success:
                c.execute("""INSERT INTO oh_ingest_log (source, last_checked_at, last_success_at, last_record_count, last_error, total_records)
                    VALUES (%s, %s, %s, %s, NULL, %s)
                    ON CONFLICT (source) DO UPDATE SET
                        last_checked_at = EXCLUDED.last_checked_at,
                        last_success_at = EXCLUDED.last_success_at,
                        last_record_count = EXCLUDED.last_record_count,
                        last_error = NULL,
                        total_records = oh_ingest_log.total_records + EXCLUDED.last_record_count""",
                    (source, now, now, record_count, record_count))
            else:
                c.execute("""INSERT INTO oh_ingest_log (source, last_checked_at, last_error)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (source) DO UPDATE SET
                        last_checked_at = EXCLUDED.last_checked_at,
                        last_error = EXCLUDED.last_error""",
                    (source, now, error or "unknown error"))
            conn.commit()
    except Exception as e:
        print("[update_ingest_log] failed for " + source + ": " + str(e))


def get_ingest_status() -> dict:
    """Return all source last-check timestamps for /health and empty-state UX."""
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT source, last_checked_at, last_success_at, last_record_count, last_error, total_records FROM oh_ingest_log")
            rows = c.fetchall()
            out = {}
            for r in rows:
                out[r["source"]] = {
                    "last_checked_at": r["last_checked_at"].isoformat() if r["last_checked_at"] else None,
                    "last_success_at": r["last_success_at"].isoformat() if r["last_success_at"] else None,
                    "last_record_count": r["last_record_count"],
                    "last_error": r["last_error"],
                    "total_records": r["total_records"],
                }
            return out
    except Exception as e:
        print("[get_ingest_status] failed: " + str(e))
        return {}


# ── INGEST: openFDA DRUG RECALLS ──────────────────────────────────────────────
def ingest_openfda_drugs(window_days: int = INGEST_WINDOW_DAYS) -> dict:
    """Pull drug recalls from openFDA /drug/enforcement.json over a rolling
    window. Upserts into oh_recalls with source='fda_drug'. Updates oh_ingest_log
    on every run, success or failure. Mirrors MW's openFDA pattern for food."""
    cutoff = (datetime.utcnow() - timedelta(days=window_days)).strftime("%Y%m%d")
    today = datetime.utcnow().strftime("%Y%m%d")
    # NOTE: openFDA needs literal '+TO+' in the search string; build URL manually
    # since requests will URL-encode the '+' as '%2B' if passed via params.
    search_str = "report_date:[" + cutoff + "+TO+" + today + "]"
    full_url = OPENFDA_DRUG_URL + "?search=" + search_str + "&limit=1000"
    if OPENFDA_KEY:
        full_url = full_url + "&api_key=" + OPENFDA_KEY
    headers = {"User-Agent": "Mozilla/5.0 (ourhealthwatch/0.1.1)"}
    inserted = 0
    skipped = 0
    try:
        r = requests.get(full_url, headers=headers, timeout=30)
        if r.status_code != 200:
            err = "HTTP " + str(r.status_code) + " " + r.text[:200]
            update_ingest_log("fda_drug", success=False, error=err)
            return {"source": "fda_drug", "error": err, "inserted": 0}
        data = r.json()
        results = data.get("results", [])
        with get_db() as conn:
            c = conn.cursor()
            for rec in results:
                rid = "fda_drug_" + (rec.get("recall_number") or rec.get("event_id") or "")
                if rid == "fda_drug_":
                    skipped = skipped + 1
                    continue
                brand = (rec.get("recalling_firm") or "").strip()
                desc = (rec.get("product_description") or "").strip()
                cls = (rec.get("classification") or "").replace("Class ", "").strip()
                reason = (rec.get("reason_for_recall") or "").strip()
                rdate = (rec.get("recall_initiation_date") or rec.get("report_date") or "").strip()
                dist = (rec.get("distribution_pattern") or "").strip()
                lots = (rec.get("code_info") or "").strip()  # often contains NDC for drugs
                status = (rec.get("status") or "").strip()
                try:
                    c.execute("""INSERT INTO oh_recalls (source, recall_id, brand, product_description, upc,
                        classification, reason, recall_date, distribution, lot_codes, status, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (recall_id) DO UPDATE SET
                            status = EXCLUDED.status, fetched_at = CURRENT_TIMESTAMP""",
                        ("fda_drug", rid, brand, desc, "", cls, reason, rdate, dist, lots, status, json.dumps(rec)))
                    inserted = inserted + 1
                except Exception as e:
                    skipped = skipped + 1
                    print("[fda_drug] skip " + rid + ": " + str(e))
            conn.commit()
        update_ingest_log("fda_drug", success=True, record_count=inserted)
        return {"source": "fda_drug", "fetched": len(results), "inserted": inserted, "skipped": skipped}
    except Exception as e:
        update_ingest_log("fda_drug", success=False, error=str(e))
        return {"source": "fda_drug", "error": str(e), "inserted": inserted}


# ── INGEST: openFDA DEVICE RECALLS ────────────────────────────────────────────
def ingest_openfda_devices(window_days: int = INGEST_WINDOW_DAYS) -> dict:
    """Pull medical device recalls from openFDA /device/enforcement.json.
    Same schema, same upsert pattern as drugs. source='fda_device'."""
    cutoff = (datetime.utcnow() - timedelta(days=window_days)).strftime("%Y%m%d")
    today = datetime.utcnow().strftime("%Y%m%d")
    search_str = "report_date:[" + cutoff + "+TO+" + today + "]"
    full_url = OPENFDA_DEVICE_URL + "?search=" + search_str + "&limit=1000"
    if OPENFDA_KEY:
        full_url = full_url + "&api_key=" + OPENFDA_KEY
    headers = {"User-Agent": "Mozilla/5.0 (ourhealthwatch/0.1.3)"}
    inserted = 0
    skipped = 0
    try:
        r = requests.get(full_url, headers=headers, timeout=30)
        if r.status_code != 200:
            err = "HTTP " + str(r.status_code) + " " + r.text[:200]
            update_ingest_log("fda_device", success=False, error=err)
            return {"source": "fda_device", "error": err, "inserted": 0}
        data = r.json()
        results = data.get("results", [])
        with get_db() as conn:
            c = conn.cursor()
            for rec in results:
                rid = "fda_device_" + (rec.get("recall_number") or rec.get("event_id") or "")
                if rid == "fda_device_":
                    skipped = skipped + 1
                    continue
                brand = (rec.get("recalling_firm") or "").strip()
                desc = (rec.get("product_description") or "").strip()
                cls = (rec.get("classification") or "").replace("Class ", "").strip()
                reason = (rec.get("reason_for_recall") or "").strip()
                rdate = (rec.get("recall_initiation_date") or rec.get("report_date") or "").strip()
                dist = (rec.get("distribution_pattern") or "").strip()
                lots = (rec.get("code_info") or "").strip()
                status = (rec.get("status") or "").strip()
                try:
                    c.execute("""INSERT INTO oh_recalls (source, recall_id, brand, product_description, upc,
                        classification, reason, recall_date, distribution, lot_codes, status, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (recall_id) DO UPDATE SET
                            status = EXCLUDED.status, fetched_at = CURRENT_TIMESTAMP""",
                        ("fda_device", rid, brand, desc, "", cls, reason, rdate, dist, lots, status, json.dumps(rec)))
                    inserted = inserted + 1
                except Exception as e:
                    skipped = skipped + 1
                    print("[fda_device] skip " + rid + ": " + str(e))
            conn.commit()
        update_ingest_log("fda_device", success=True, record_count=inserted)
        return {"source": "fda_device", "fetched": len(results), "inserted": inserted, "skipped": skipped}
    except Exception as e:
        update_ingest_log("fda_device", success=False, error=str(e))
        return {"source": "fda_device", "error": str(e), "inserted": inserted}


# ── INGEST: CDC NORS (foodborne / waterborne outbreaks) ───────────────────────
def ingest_nors(limit: int = NORS_FETCH_LIMIT) -> dict:
    """Pull NORS outbreaks from data.cdc.gov Socrata API (5xkq-dg7x).
    NORS publishes annual aggregates per outbreak: year, month, state,
    etiology, primary_mode, illnesses, hospitalizations, deaths.
    Stored in oh_outbreaks with source='cdc_nors'."""
    url = NORS_URL + "?$order=year DESC&$limit=" + str(limit)
    headers = {"User-Agent": "Mozilla/5.0 (ourhealthwatch/0.1.3)", "Accept": "application/json"}
    inserted = 0
    skipped = 0
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            err = "HTTP " + str(r.status_code) + " " + r.text[:200]
            update_ingest_log("cdc_nors", success=False, error=err)
            return {"source": "cdc_nors", "error": err, "inserted": 0}
        rows = r.json()
        with get_db() as conn:
            c = conn.cursor()
            for rec in rows:
                # Socrata returns string year/month; construct a stable outbreak_id
                year = (rec.get("year") or "").strip() if isinstance(rec.get("year"), str) else str(rec.get("year") or "")
                month = (rec.get("month") or "").strip() if isinstance(rec.get("month"), str) else str(rec.get("month") or "")
                state = (rec.get("state") or rec.get("primary_state") or "").strip()
                etiology = (rec.get("etiology") or rec.get("genus") or "Unknown").strip()
                mode = (rec.get("primary_mode") or rec.get("transmission_mode") or "").strip()
                setting = (rec.get("setting") or "").strip()
                cdc_id = (rec.get("cdc_id") or rec.get("outbreak_id") or "").strip()
                oid_seed = cdc_id if cdc_id else (year + "-" + month + "-" + state + "-" + etiology)
                oid = "cdc_nors_" + oid_seed.replace(" ", "_")[:120]
                if oid == "cdc_nors_":
                    skipped = skipped + 1
                    continue
                try:
                    illnesses = int(rec.get("illnesses") or rec.get("ill_total") or 0)
                except (TypeError, ValueError):
                    illnesses = 0
                title_parts = []
                if etiology and etiology != "Unknown":
                    title_parts.append(etiology)
                if mode:
                    title_parts.append("(" + mode + ")")
                if state:
                    title_parts.append("in " + state)
                title = " ".join(title_parts) if title_parts else "NORS outbreak"
                report_date = (year + "-" + month.zfill(2) if year and month else year) or ""
                summary_bits = []
                if setting:
                    summary_bits.append("Setting: " + setting)
                summary_bits.append("Illnesses: " + str(illnesses))
                try:
                    hosp = int(rec.get("hospitalizations") or 0)
                    if hosp:
                        summary_bits.append("Hospitalizations: " + str(hosp))
                except (TypeError, ValueError):
                    pass
                try:
                    deaths = int(rec.get("deaths") or 0)
                    if deaths:
                        summary_bits.append("Deaths: " + str(deaths))
                except (TypeError, ValueError):
                    pass
                summary = " · ".join(summary_bits)
                try:
                    c.execute("""INSERT INTO oh_outbreaks (source, outbreak_id, title, agent, location,
                        country_code, region, ship_name, cruise_line, cases, report_date, report_url, summary, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (outbreak_id) DO UPDATE SET
                            cases = EXCLUDED.cases, summary = EXCLUDED.summary, fetched_at = CURRENT_TIMESTAMP""",
                        ("cdc_nors", oid, title, etiology, state, "US", state, None, None,
                         illnesses, report_date, "https://wwwn.cdc.gov/norsdashboard/", summary, json.dumps(rec)))
                    inserted = inserted + 1
                except Exception as e:
                    skipped = skipped + 1
                    print("[cdc_nors] skip " + oid + ": " + str(e))
            conn.commit()
        update_ingest_log("cdc_nors", success=True, record_count=inserted)
        return {"source": "cdc_nors", "fetched": len(rows), "inserted": inserted, "skipped": skipped}
    except Exception as e:
        update_ingest_log("cdc_nors", success=False, error=str(e))
        return {"source": "cdc_nors", "error": str(e), "inserted": inserted}


# ── AUTH HELPERS ──────────────────────────────────────────────────────────────
def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(pw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def make_jwt(user_id: int, email: str) -> str:
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(days=JWT_EXPIRY_DAYS),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGO)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def decode_jwt(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGO])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def require_user(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization[7:].strip()
    payload = decode_jwt(token)
    return {"id": payload.get("user_id"), "email": payload.get("email")}


def require_admin(x_admin_key: Optional[str] = Header(None)):
    expected = os.environ.get("ADMIN_KEY", "")
    if not expected or x_admin_key != expected:
        raise HTTPException(status_code=403, detail="Admin only")
    return True


# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────
class RegisterIn(BaseModel):
    email: EmailStr
    password: str


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class WatchlistAddIn(BaseModel):
    kind: Optional[str] = "product"
    brand: Optional[str] = None
    product_name: Optional[str] = None
    upc: Optional[str] = None
    keyword: Optional[str] = None
    category: Optional[str] = None


# ── HEALTH / META ─────────────────────────────────────────────────────────────
@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": API_VERSION,
        "ingest_status": get_ingest_status(),
    }


# ── AUTH ENDPOINTS ────────────────────────────────────────────────────────────
@app.post("/auth/register")
async def register(body: RegisterIn):
    pw_hash = hash_password(body.password)
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO oh_users (email, password_hash) VALUES (%s, %s) RETURNING id",
                (body.email, pw_hash))
            user_id = c.fetchone()[0]
            conn.commit()
        token = make_jwt(user_id, body.email)
        return {"token": token, "user_id": user_id, "email": body.email}
    except psycopg2.errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="Email already registered")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Registration failed: " + str(e))


@app.post("/auth/login")
async def login(body: LoginIn):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT id, password_hash FROM oh_users WHERE email = %s", (body.email,))
            row = c.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        user_id, pw_hash = row
        if not verify_password(body.password, pw_hash):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        token = make_jwt(user_id, body.email)
        return {"token": token, "user_id": user_id, "email": body.email}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Login failed: " + str(e))


@app.get("/account")
async def account(user=Depends(require_user)):
    return {"user_id": user["id"], "email": user["email"]}


@app.delete("/account")
async def delete_account(user=Depends(require_user)):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM oh_notifications WHERE user_id = %s", (user["id"],))
            c.execute("DELETE FROM oh_watchlist WHERE user_id = %s", (user["id"],))
            c.execute("DELETE FROM oh_users WHERE id = %s", (user["id"],))
            conn.commit()
        return {"status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Delete failed: " + str(e))


# ── WATCHLIST ENDPOINTS (skeleton — populated as adapters land) ───────────────
@app.get("/watchlist")
async def list_watchlist(user=Depends(require_user)):
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""SELECT id, kind, brand, product_name, upc, keyword, category,
                monitoring, has_alert, last_match_id, last_checked, status, created_at
                FROM oh_watchlist WHERE user_id = %s ORDER BY created_at DESC""", (user["id"],))
            rows = c.fetchall()
        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "kind": r["kind"],
                "brand": r["brand"],
                "product_name": r["product_name"],
                "upc": r["upc"],
                "keyword": r["keyword"],
                "category": r["category"],
                "monitoring": r["monitoring"],
                "has_alert": r["has_alert"],
                "last_match_id": r["last_match_id"],
                "last_checked": r["last_checked"].isoformat() if r["last_checked"] else None,
                "status": r["status"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            })
        return {"results": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Watchlist fetch failed: " + str(e))


@app.post("/watchlist")
async def add_watchlist(body: WatchlistAddIn, user=Depends(require_user)):
    if not (body.brand or body.product_name or body.keyword):
        raise HTTPException(status_code=400, detail="Provide at least one of: brand, product_name, keyword")
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""INSERT INTO oh_watchlist (user_id, kind, brand, product_name, upc, keyword, category)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (user["id"], body.kind or "product", body.brand, body.product_name,
                 body.upc, body.keyword, body.category))
            new_id = c.fetchone()[0]
            conn.commit()
        return {"id": new_id, "status": "added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Add failed: " + str(e))


@app.delete("/watchlist/{item_id}")
async def delete_watchlist(item_id: int, user=Depends(require_user)):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM oh_notifications WHERE watchlist_id = %s AND user_id = %s",
                (item_id, user["id"]))
            c.execute("DELETE FROM oh_watchlist WHERE id = %s AND user_id = %s",
                (item_id, user["id"]))
            conn.commit()
        return {"status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Delete failed: " + str(e))


def find_best_recall_for_watch(conn, brand: Optional[str], product_name: Optional[str],
                                upc: Optional[str], keyword: Optional[str]):
    """Return best matching oh_recalls row for a watchlist entry, or None.
    Forked from MW pattern: UPC exact → brand/keyword fuzzy with scoring.
    Threshold of 30 keeps it from surfacing weak matches."""
    c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # UPC exact match (when both have it)
    if upc:
        c.execute("SELECT * FROM oh_recalls WHERE upc = %s AND status = 'Ongoing' LIMIT 1", (upc,))
        row = c.fetchone()
        if row:
            return dict(row)
    # Brand or keyword fuzzy
    primary = brand or keyword
    if primary:
        like = "%" + primary + "%"
        c.execute("""SELECT * FROM oh_recalls
            WHERE (brand ILIKE %s OR product_description ILIKE %s)
              AND status = 'Ongoing'
            ORDER BY recall_date DESC NULLS LAST LIMIT 50""", (like, like))
        rows = c.fetchall()
        best = None
        best_score = 0
        for row in rows:
            d = dict(row)
            s = 0
            b = (d.get("brand") or "").lower()
            desc = (d.get("product_description") or "").lower()
            p = primary.lower()
            pn = (product_name or "").lower()
            if p and p in b:
                s = s + (50 if b == p else 30)
            elif p and p in desc:
                s = s + 25
            if pn and pn in desc:
                s = s + 20
            elif pn and pn in b:
                s = s + 10
            if (d.get("classification") or "").startswith("I"):
                s = s + 5
            # require primary term to actually match — don't surface unrelated recalls
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


@app.get("/watchlist/{item_id}/refresh")
async def refresh_watchlist_item(item_id: int, user=Depends(require_user)):
    """On-demand recheck for a single watchlist item — used by the drawer.
    Looks up the best matching Ongoing recall in oh_recalls and updates
    has_alert + last_match_id + last_checked on the watchlist row."""
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""SELECT id, brand, product_name, upc, keyword, kind, category
                FROM oh_watchlist WHERE id = %s AND user_id = %s AND status = 'active'""",
                (item_id, user["id"]))
            wrow = c.fetchone()
            if not wrow:
                raise HTTPException(status_code=404, detail="Watchlist item not found")
            best = find_best_recall_for_watch(conn, wrow["brand"], wrow["product_name"],
                                               wrow["upc"], wrow["keyword"])
            now = datetime.utcnow()
            has_alert = best is not None
            last_match_id = best["recall_id"] if best else None
            uc = conn.cursor()
            uc.execute("""UPDATE oh_watchlist
                SET has_alert = %s, last_match_id = %s, last_checked = %s
                WHERE id = %s""", (has_alert, last_match_id, now, item_id))
            conn.commit()
            return {
                "id": item_id,
                "has_alert": has_alert,
                "matched": has_alert,
                "recall": serialize_recall(best),
                "last_checked": now.isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Refresh failed: " + str(e))


# ── RECALLS ENDPOINTS (firehose + opt-in pattern) ─────────────────────────────
@app.get("/recalls/recent")
async def recent_recalls(
    source: Optional[str] = None,
    limit: int = 25,
    status: str = "Ongoing",
    include_all_status: bool = False,
):
    """The 'show all' firehose. Default: last 30 days, Ongoing status only,
    limit 25. Frontend renders this with an opt-in 'Add to Watchlist' button
    on each card. include_all_status=true bypasses the Ongoing filter."""
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            where = ["1=1"]
            params: list = []
            if source:
                where.append("source = %s")
                params.append(source)
            else:
                where.append("source LIKE 'fda_%%'")
            if not include_all_status:
                where.append("status = %s")
                params.append(status)
            params.append(int(limit))
            sql = ("SELECT id, source, recall_id, brand, product_description, upc, "
                   "classification, reason, recall_date, distribution, lot_codes, status, fetched_at "
                   "FROM oh_recalls WHERE " + " AND ".join(where) +
                   " ORDER BY recall_date DESC NULLS LAST, fetched_at DESC LIMIT %s")
            c.execute(sql, tuple(params))
            rows = c.fetchall()
        items = []
        for r in rows:
            items.append({
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
        # Include per-source last_checked timestamps so the UI can render
        # "Last checked X min ago" even on an empty list. Source filter, if
        # passed, scopes the timestamp to just that source.
        ingest = get_ingest_status()
        if source and source in ingest:
            checked = {source: ingest[source]}
        else:
            checked = {k: v for k, v in ingest.items() if k.startswith("fda_")}
        return {"results": items, "ingest_status": checked, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Recent recalls fetch failed: " + str(e))


# ── OUTBREAKS ENDPOINTS (firehose + opt-in pattern) ───────────────────────────
@app.get("/outbreaks/recent")
async def recent_outbreaks(
    source: Optional[str] = None,
    limit: int = 25,
    agent: Optional[str] = None,
    region: Optional[str] = None,
):
    """Outbreak firehose. Source can be 'cdc_nors', 'who_don', 'cdc_vsp', etc.
    Optional filters by agent (etiology) and region (state/country).
    Adapters land progressively: NORS in v0.1.3, WHO DON in v0.1.5, VSP later."""
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            where = ["1=1"]
            params: list = []
            if source:
                where.append("source = %s")
                params.append(source)
            if agent:
                where.append("agent ILIKE %s")
                params.append("%" + agent + "%")
            if region:
                where.append("region ILIKE %s")
                params.append("%" + region + "%")
            params.append(int(limit))
            sql = ("SELECT id, source, outbreak_id, title, agent, location, country_code, region, "
                   "ship_name, cruise_line, cases, report_date, report_url, summary, fetched_at "
                   "FROM oh_outbreaks WHERE " + " AND ".join(where) +
                   " ORDER BY report_date DESC NULLS LAST, fetched_at DESC LIMIT %s")
            c.execute(sql, tuple(params))
            rows = c.fetchall()
        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "source": r["source"],
                "outbreak_id": r["outbreak_id"],
                "title": r["title"],
                "agent": r["agent"],
                "location": r["location"],
                "country_code": r["country_code"],
                "region": r["region"],
                "ship_name": r["ship_name"],
                "cruise_line": r["cruise_line"],
                "cases": r["cases"],
                "report_date": r["report_date"],
                "report_url": r["report_url"],
                "summary": r["summary"],
                "fetched_at": r["fetched_at"].isoformat() if r["fetched_at"] else None,
            })
        ingest = get_ingest_status()
        if source and source in ingest:
            checked = {source: ingest[source]}
        else:
            checked = {k: v for k, v in ingest.items()
                       if k.startswith("cdc_") or k.startswith("who_")}
        return {"results": items, "ingest_status": checked, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Recent outbreaks fetch failed: " + str(e))


# ── NOTIFICATIONS ENDPOINTS ───────────────────────────────────────────────────
@app.get("/notifications")
async def list_notifications(user=Depends(require_user)):
    try:
        with get_db() as conn:
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""SELECT id, watchlist_id, message, source, source_ref_id, email_sent, created_at
                FROM oh_notifications WHERE user_id = %s ORDER BY created_at DESC LIMIT 100""",
                (user["id"],))
            rows = c.fetchall()
        items = []
        for r in rows:
            items.append({
                "id": r["id"],
                "watchlist_id": r["watchlist_id"],
                "message": r["message"],
                "source": r["source"],
                "source_ref_id": r["source_ref_id"],
                "email_sent": r["email_sent"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            })
        return {"results": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Notifications fetch failed: " + str(e))


@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: int, user=Depends(require_user)):
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM oh_notifications WHERE id = %s AND user_id = %s",
                (notif_id, user["id"]))
            conn.commit()
        return {"status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Delete failed: " + str(e))


# ── ADMIN ENDPOINTS ───────────────────────────────────────────────────────────
@app.get("/admin/signup-stats")
async def admin_signup_stats(x_admin_token: str = Header(None, alias="X-Admin-Token")):
    # Uses ADMIN_STATS_KEY (separate from ADMIN_KEY) so read-only stats access
    # can be shared with partners/team without granting operational control.
    expected = os.environ.get("ADMIN_STATS_KEY", "")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="Admin only")
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM oh_users")
            total = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM oh_users WHERE created_at >= NOW() - INTERVAL '7 days'")
            week = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM oh_users WHERE created_at >= NOW() - INTERVAL '24 hours'")
            day = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM oh_watchlist")
            watch = c.fetchone()[0]
        return {
            "users_total": total,
            "users_last_7d": week,
            "users_last_24h": day,
            "watchlist_items_total": watch,
            "version": API_VERSION,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Stats failed: " + str(e))


@app.get("/admin/ingest-status")
async def admin_ingest_status(_admin=Depends(require_admin)):
    """Inspect per-source ingest health. Useful when an adapter goes quiet."""
    return {"ingest_status": get_ingest_status(), "checked_at": datetime.utcnow().isoformat()}


@app.post("/admin/refresh-recalls")
async def admin_refresh_recalls(_admin=Depends(require_admin)):
    """Manual trigger for openFDA recall ingests (drug + device). Useful for
    forcing a refresh between cron ticks or smoke-testing."""
    drug_res = ingest_openfda_drugs()
    device_res = ingest_openfda_devices()
    return {"drug": drug_res, "device": device_res, "ran_at": datetime.utcnow().isoformat()}


@app.post("/admin/refresh-outbreaks")
async def admin_refresh_outbreaks(_admin=Depends(require_admin)):
    """Manual trigger for outbreak ingests (NORS + future WHO DON, VSP).
    Returns per-source result dicts."""
    nors_res = ingest_nors()
    return {"nors": nors_res, "ran_at": datetime.utcnow().isoformat()}


# ── CRON ──────────────────────────────────────────────────────────────────────
def run_watchlist_check():
    """Background tick. v0.1.3 runs openFDA drug+device recall ingest plus
    CDC NORS foodborne/waterborne outbreaks. Future adapters (WHO DON,
    State Dept, VSP) land progressively. Each adapter calls update_ingest_log()
    on completion, success or failure."""
    print("[cron] tick at " + datetime.utcnow().isoformat())
    try:
        drug_res = ingest_openfda_drugs()
        print("[cron] fda_drug: " + json.dumps(drug_res))
    except Exception as e:
        print("[cron] fda_drug exception: " + str(e))
        update_ingest_log("fda_drug", success=False, error=str(e))
    try:
        device_res = ingest_openfda_devices()
        print("[cron] fda_device: " + json.dumps(device_res))
    except Exception as e:
        print("[cron] fda_device exception: " + str(e))
        update_ingest_log("fda_device", success=False, error=str(e))
    try:
        nors_res = ingest_nors()
        print("[cron] cdc_nors: " + json.dumps(nors_res))
    except Exception as e:
        print("[cron] cdc_nors exception: " + str(e))
        update_ingest_log("cdc_nors", success=False, error=str(e))
    update_ingest_log("system", success=True, record_count=0)


def run_scheduler():
    schedule.every(WATCHLIST_CHECK_INTERVAL_HOURS).hours.do(run_watchlist_check)
    # Run an initial ingest on startup so /recalls/recent has fresh data
    # immediately rather than waiting up to 12 hrs for the first cron tick.
    try:
        run_watchlist_check()
    except Exception as e:
        print("[scheduler] startup ingest failed: " + str(e))
    while True:
        schedule.run_pending()
        time_mod.sleep(60)


# ── STARTUP ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    init_db()
    # Seed system row in ingest_log so /health has something to show on day one.
    update_ingest_log("system", success=True, record_count=0)
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()
    print("OurHealth.Watch API v" + API_VERSION + " started (cron thread up)")
