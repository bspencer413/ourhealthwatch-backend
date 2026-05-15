# ============================================================================
# OHW BACKEND PATCH — v0.1.31 → v0.1.32
# ============================================================================
# Two changes in main.py:
#
#   1. Bump API_VERSION constant:
#         OLD:  API_VERSION = "0.1.31"
#         NEW:  API_VERSION = "0.1.32"
#
#   2. Replace the entire admin_signup_stats() function with the version below.
#      Find the existing function (search for "async def admin_signup_stats")
#      and replace from the @app.get("/admin/signup-stats") decorator through
#      the trailing "raise HTTPException..." line.
#
# Commit, Render auto-redeploys, scoreboard card lights up.
# ============================================================================


@app.get("/admin/signup-stats")
async def admin_signup_stats(x_admin_token: str = Header(None, alias="X-Admin-Token")):
    # Uses ADMIN_STATS_KEY (separate from ADMIN_KEY) so read-only stats access
    # can be shared with partners/team without granting operational control.
    # v0.1.32: added scoreboard-standard keys (total_users, signups_24h/7d/30d,
    # latest_signup_at) so the 3Brains scoreboard reads OHW like the other apps.
    # Legacy OHW-specific keys kept for backward-compat with any internal UI.
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
            c.execute("SELECT COUNT(*) FROM oh_users WHERE created_at >= NOW() - INTERVAL '30 days'")
            month = c.fetchone()[0]
            c.execute("SELECT MAX(created_at) FROM oh_users")
            latest_row = c.fetchone()
            latest = latest_row[0] if latest_row and latest_row[0] else None
            c.execute("SELECT COUNT(*) FROM oh_watchlist")
            watch = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM oh_places")
            places = c.fetchone()[0]
        return {
            # Scoreboard-standard keys (match Player Watch, Cruise, Earth, etc.)
            "total_users": total,
            "signups_24h": day,
            "signups_7d": week,
            "signups_30d": month,
            "latest_signup_at": latest.isoformat() if latest else None,
            # OHW-specific legacy keys (kept for backward-compat with any internal UI)
            "users_total": total,
            "users_last_7d": week,
            "users_last_24h": day,
            "watchlist_items_total": watch,
            "places_total": places,
            "version": API_VERSION,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Stats failed: " + str(e))
