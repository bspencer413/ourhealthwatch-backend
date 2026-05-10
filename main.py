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

API_VERSION = "0.1.0"
JWT_ALGO = "HS256"
JWT_EXPIRY_DAYS = 7
WATCHLIST_CHECK_INTERVAL_HOURS = 12  # free tier; premium will be 1hr

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


# ── CRON SKELETON ─────────────────────────────────────────────────────────────
def run_watchlist_check():
    """Background watchlist refresh. v0.1.0 is a stub — no adapters yet.
    Drug recall ingest lands v0.1.1, NORS v0.1.4, WHO DON v0.1.5, etc.
    Each adapter calls update_ingest_log() on completion."""
    print("[cron] watchlist check tick at " + datetime.utcnow().isoformat() + " (v0.1.0 stub — adapters land in v0.1.1+)")
    update_ingest_log("system", success=True, record_count=0)


def run_scheduler():
    schedule.every(WATCHLIST_CHECK_INTERVAL_HOURS).hours.do(run_watchlist_check)
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
