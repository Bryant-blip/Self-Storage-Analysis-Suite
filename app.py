"""
app.py — Storage Intel Web Dashboard

Flask backend for the self-storage deal analysis dashboard.

Usage:
    python app.py
    → opens at http://localhost:5000
"""

import glob
import json
import os
import queue
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_file, send_from_directory, abort, Response, stream_with_context
from dotenv import load_dotenv

load_dotenv()

import comps_pipeline
from db_utils import get_db, init_db

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(PROJECT_DIR, "data")

app = Flask(__name__)


@app.before_request
def _ensure_db():
    """Lazy-init DB on first request so we don't need app context tricks."""
    pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _date_clause(date_range: str, col: str = "processed_at") -> str:
    """Return SQL WHERE fragment for date_range param (30d / 90d / all)."""
    if date_range == "30d":
        return f"{col} >= datetime('now', '-30 days')"
    if date_range == "90d":
        return f"{col} >= datetime('now', '-90 days')"
    return "1=1"


def _row_to_dict(row) -> dict:
    return dict(row) if row else {}


def _rows_to_list(rows) -> list:
    return [dict(r) for r in rows]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/api/states")
def api_states():
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT market AS state,
                   COUNT(*) AS deal_count,
                   AVG(yield_on_cost) AS avg_yoc,
                   AVG(avg_psf) AS avg_psf,
                   AVG(population_3mi) AS avg_population
            FROM deals
            WHERE processed_at IS NOT NULL AND skip_reason IS NULL
              AND market IS NOT NULL AND market != ''
            GROUP BY market
            ORDER BY deal_count DESC
        """).fetchall()
        return jsonify(_rows_to_list(rows))
    finally:
        conn.close()


@app.route("/api/overview")
def api_overview():
    state      = request.args.get("state", "").strip()
    date_range = request.args.get("date_range", "all")

    date_sql  = _date_clause(date_range)
    state_sql = "AND market = ?" if state else ""
    params    = [state] if state else []

    conn = get_db()
    try:
        where = f"WHERE processed_at IS NOT NULL AND skip_reason IS NULL AND {date_sql} {state_sql}"

        summary = conn.execute(f"""
            SELECT
                COUNT(*)                        AS total_deals,
                COUNT(DISTINCT market)          AS total_markets,
                AVG(yield_on_cost)              AS avg_yoc,
                AVG(avg_psf)                    AS avg_psf,
                AVG(price_per_acre)             AS avg_price_per_acre,
                AVG(population_3mi)             AS avg_population,
                SUM(CASE WHEN population_3mi >= 50000 THEN 1 ELSE 0 END) AS zip_count_above_50k
            FROM deals {where}
        """, params).fetchone()

        by_state = conn.execute(f"""
            SELECT market AS state, COUNT(*) AS deal_count,
                   AVG(avg_psf) AS avg_psf,
                   AVG(yield_on_cost) AS avg_yoc,
                   AVG(price_per_acre) AS avg_price_per_acre
            FROM deals {where}
            GROUP BY market ORDER BY deal_count DESC
        """, params).fetchall()

        result = _row_to_dict(summary)
        by_state_list = _rows_to_list(by_state)
        result["deals_by_state"]          = by_state_list
        result["avg_psf_by_state"]        = {r["state"]: r["avg_psf"]          for r in by_state_list}
        result["avg_yoc_by_state"]        = {r["state"]: r["avg_yoc"]          for r in by_state_list}
        result["avg_price_per_acre_by_state"] = {r["state"]: r["avg_price_per_acre"] for r in by_state_list}
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/deals")
def api_deals():
    state       = request.args.get("state", "").strip()
    min_yoc     = request.args.get("min_yoc", "").strip()
    min_pop     = request.args.get("min_population", "").strip()
    date_range  = request.args.get("date_range", "all")
    sort_by     = request.args.get("sort_by", "deal_score")
    unit_size   = request.args.get("unit_size", "").strip()
    unit_type   = request.args.get("unit_type", "").strip()

    # Allowed sort columns
    sort_map = {
        "deal_score":     "d.deal_score DESC NULLS LAST",
        "yoc":            "d.yield_on_cost DESC NULLS LAST",
        "psf":            "d.avg_psf DESC NULLS LAST",
        "population":     "d.population_3mi DESC NULLS LAST",
        "price_per_acre": "d.price_per_acre ASC NULLS LAST",
        "date":           "d.processed_at DESC",
    }
    order = sort_map.get(sort_by, "d.deal_score DESC NULLS LAST")

    clauses = [
        "d.processed_at IS NOT NULL",
        "d.skip_reason IS NULL",
        _date_clause(date_range, "d.processed_at"),
    ]
    params = []

    if state:
        clauses.append("d.market = ?")
        params.append(state)
    if min_yoc:
        try:
            clauses.append("d.yield_on_cost >= ?")
            params.append(float(min_yoc) / 100)
        except ValueError:
            pass
    if min_pop:
        try:
            clauses.append("d.population_3mi >= ?")
            params.append(int(min_pop))
        except ValueError:
            pass

    # Join comps if unit size / type filter needed
    if unit_size or unit_type:
        join_sql = "JOIN comps c ON d.listing_id = c.listing_id"
        if unit_size:
            clauses.append("c.unit_size = ?")
            params.append(unit_size)
        if unit_type:
            clauses.append("c.unit_type = ?")
            params.append(unit_type)
        select_prefix = "SELECT DISTINCT d.*"
    else:
        join_sql = ""
        select_prefix = "SELECT d.*"

    where = "WHERE " + " AND ".join(clauses)
    sql = f"{select_prefix} FROM deals d {join_sql} {where} ORDER BY {order}"

    conn = get_db()
    try:
        rows = conn.execute(sql, params).fetchall()
        return jsonify(_rows_to_list(rows))
    finally:
        conn.close()


@app.route("/api/trends")
def api_trends():
    state      = request.args.get("state", "").strip()
    metric     = request.args.get("metric", "psf")
    date_range = request.args.get("date_range", "all")

    metric_map = {
        "psf":           "AVG(avg_psf)",
        "price_per_acre": "AVG(price_per_acre)",
        "population":    "AVG(population_3mi)",
        "deal_count":    "COUNT(*)",
    }
    agg = metric_map.get(metric, "AVG(avg_psf)")

    clauses = [
        "processed_at IS NOT NULL",
        "skip_reason IS NULL",
        _date_clause(date_range),
    ]
    params = []
    if state:
        clauses.append("market = ?")
        params.append(state)

    where = "WHERE " + " AND ".join(clauses)
    sql = f"""
        SELECT strftime('%Y-W%W', processed_at) AS date,
               market AS state,
               {agg} AS value
        FROM deals
        {where}
        GROUP BY date, market
        ORDER BY date ASC
    """

    conn = get_db()
    try:
        rows = conn.execute(sql, params).fetchall()
        return jsonify(_rows_to_list(rows))
    finally:
        conn.close()


@app.route("/api/city/<path:city_name>")
def api_city(city_name):
    conn = get_db()
    try:
        summary = conn.execute("""
            SELECT COUNT(*) AS deal_count,
                   AVG(avg_psf) AS avg_psf,
                   AVG(yield_on_cost) AS avg_yoc,
                   AVG(population_3mi) AS avg_population,
                   AVG(price_per_acre) AS avg_price_per_acre,
                   market AS state
            FROM deals
            WHERE city_name = ? AND processed_at IS NOT NULL AND skip_reason IS NULL
        """, (city_name,)).fetchone()

        deals = conn.execute("""
            SELECT listing_id, address, market, avg_psf, yield_on_cost,
                   population_3mi, deal_score, asking_price, acres, processed_at,
                   report_path, pop_gate_passed
            FROM deals
            WHERE city_name = ? AND processed_at IS NOT NULL AND skip_reason IS NULL
            ORDER BY deal_score DESC NULLS LAST
        """, (city_name,)).fetchall()

        # Facilities: pivot web_rate by unit_size
        listing_ids = [r["listing_id"] for r in deals]
        facilities_raw = []
        if listing_ids:
            placeholders = ",".join("?" * len(listing_ids))
            fac_rows = conn.execute(f"""
                SELECT facility_name, facility_address,
                       MIN(distance_miles) AS distance_miles,
                       unit_size, AVG(web_rate) AS avg_web_rate
                FROM comps
                WHERE listing_id IN ({placeholders})
                GROUP BY facility_name, unit_size
                ORDER BY distance_miles ASC
            """, listing_ids).fetchall()

            # Pivot: facility → {size: rate}
            fac_map = {}
            for r in fac_rows:
                key = (r["facility_name"], r["facility_address"], r["distance_miles"])
                if key not in fac_map:
                    fac_map[key] = {
                        "facility_name":    r["facility_name"],
                        "facility_address": r["facility_address"],
                        "distance_miles":   r["distance_miles"],
                        "rates": {},
                    }
                fac_map[key]["rates"][r["unit_size"]] = r["avg_web_rate"]

            # Sort by distance, take top 10
            facilities_raw = sorted(fac_map.values(), key=lambda x: x["distance_miles"] or 9999)[:10]

        return jsonify({
            "summary":    _row_to_dict(summary),
            "facilities": facilities_raw,
            "deals":      _rows_to_list(deals),
        })
    finally:
        conn.close()


@app.route("/api/report/<listing_id>")
def api_report(listing_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT report_path FROM deals WHERE listing_id = ?", (listing_id,)
        ).fetchone()
    finally:
        conn.close()

    if not row or not row["report_path"]:
        abort(404, description="Report not found in database")

    path = row["report_path"]
    if not os.path.exists(path):
        abort(404, description=f"Report file not found: {path}")

    return send_file(path, as_attachment=True,
                     download_name=os.path.basename(path))


# ── Watcher (Find Deals) ──────────────────────────────────────────────────────
#
# Runs crexi_watcher.py as a subprocess. Streams stdout back to the browser
# via Server-Sent Events. Job state lives in memory (_JOBS); run metadata is
# persisted in the watcher_runs table so history survives Flask restarts.

_JOBS: dict = {}         # job_id → {proc, log_deque, subscribers, market, ...}
_JOBS_LOCK = threading.Lock()
_LOG_MAX = 2000          # max log lines retained per job


def _load_markets() -> list[str]:
    sys.path.insert(0, PROJECT_DIR)
    from crexi.scraper import STATE_ABBREVIATIONS
    return sorted(k.title() for k in STATE_ABBREVIATIONS)


def _load_counters() -> dict:
    """Read all seen_deals_*.json files → per-market counters."""
    per_market = sorted(glob.glob(os.path.join(DATA_DIR, "seen_deals_*.json")))
    legacy     = glob.glob(os.path.join(DATA_DIR, "seen_deals.json"))
    paths      = per_market + legacy

    seen_ids: set = set()
    counters: dict = {}

    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        for lid, entry in data.items():
            if lid in seen_ids:
                continue
            seen_ids.add(lid)
            market = (entry.get("market") or "").strip() or "(unknown)"
            c = counters.setdefault(market, {"total": 0, "processed": 0, "skipped": 0, "pending": 0})
            c["total"] += 1
            if entry.get("processed"):
                c["processed"] += 1
            elif entry.get("skip_reason"):
                c["skipped"] += 1
            else:
                c["pending"] += 1
    return counters


def _reader_thread(job_id: str, proc: subprocess.Popen):
    """Read proc stdout line-by-line; push into deque + fan out to subscribers."""
    job = _JOBS[job_id]
    try:
        for line in proc.stdout:
            line = line.rstrip("\r\n")
            job["log"].append(line)
            # Fan out to live subscribers
            dead = []
            for q in job["subscribers"]:
                try:
                    q.put_nowait(line)
                except Exception:
                    dead.append(q)
            for q in dead:
                job["subscribers"].discard(q)
    except Exception:
        pass
    finally:
        proc.wait()
        _finalize_job(job_id, proc.returncode)


def _finalize_job(job_id: str, exit_code: int):
    job = _JOBS.get(job_id)
    if not job:
        return
    stopped = job.get("stopped", False)
    status = "stopped" if stopped else ("finished" if exit_code == 0 else "error")
    finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Count deals added after this run started
    try:
        conn = get_db()
        deals_found = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE processed_at >= ? AND market = ?",
            (job["started_at"], job["market"]),
        ).fetchone()[0]
        conn.execute("""
            UPDATE watcher_runs
               SET status = ?, finished_at = ?, exit_code = ?, deals_found = ?
             WHERE job_id = ?
        """, (status, finished_at, exit_code, deals_found, job_id))
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"[watcher] finalize DB write failed: {exc}", file=sys.stderr)
        deals_found = 0

    job["status"] = status
    job["finished_at"] = finished_at
    job["deals_found"] = deals_found
    # Tell subscribers we're done
    for q in list(job["subscribers"]):
        try:
            q.put_nowait({"_done": True, "status": status, "deals_found": deals_found})
        except Exception:
            pass


@app.route("/api/watcher/markets")
def api_watcher_markets():
    markets  = _load_markets()
    counters = _load_counters()
    return jsonify({
        "markets":  markets,
        "counters": counters,
    })


@app.route("/api/watcher/run", methods=["POST"])
def api_watcher_run():
    body      = request.get_json(silent=True) or {}
    market    = (body.get("market") or "").strip()
    md = body.get("max_deals")
    max_deals = int(md) if md is not None and md != "" else 3
    dry_run   = bool(body.get("dry_run"))

    if not market:
        return jsonify({"error": "market required"}), 400

    job_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    cmd = [sys.executable, "-u", "crexi_watcher.py",
           "--market", market, "--max-deals", str(max_deals)]
    if dry_run:
        cmd.append("--dry-run")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            cwd=PROJECT_DIR,
            bufsize=1,
        )
    except Exception as exc:
        return jsonify({"error": f"failed to launch: {exc}"}), 500

    with _JOBS_LOCK:
        _JOBS[job_id] = {
            "proc":        proc,
            "market":      market,
            "max_deals":   max_deals,
            "dry_run":     dry_run,
            "started_at":  started_at,
            "finished_at": None,
            "status":      "running",
            "stopped":     False,
            "deals_found": 0,
            "log":         deque(maxlen=_LOG_MAX),
            "subscribers": set(),
        }

    conn = get_db()
    conn.execute("""
        INSERT INTO watcher_runs
            (job_id, market, max_deals, dry_run, status, started_at)
        VALUES (?, ?, ?, ?, 'running', ?)
    """, (job_id, market, max_deals, 1 if dry_run else 0, started_at))
    conn.commit()
    conn.close()

    threading.Thread(target=_reader_thread, args=(job_id, proc), daemon=True).start()

    return jsonify({"job_id": job_id, "market": market, "started_at": started_at})


@app.route("/api/watcher/stop/<job_id>", methods=["POST"])
def api_watcher_stop(job_id):
    job = _JOBS.get(job_id)
    if not job:
        return jsonify({"error": "unknown job"}), 404
    proc = job["proc"]
    # Flip in-memory + DB status immediately so the UI updates on the next poll,
    # even if the reader thread takes a moment to detect EOF.
    job["stopped"] = True
    job["status"]  = "stopped"
    try:
        conn = get_db()
        conn.execute("UPDATE watcher_runs SET status = 'stopped' WHERE job_id = ?", (job_id,))
        conn.commit()
        conn.close()
    except Exception:
        pass

    if proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
        # Escalate to kill if terminate doesn't land within 3s — otherwise a
        # stuck child (e.g. blocked on a network read) would keep the job alive.
        def _force_kill():
            time.sleep(3)
            if proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass
        threading.Thread(target=_force_kill, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/watcher/active")
def api_watcher_active():
    """Currently-running jobs only. Finished jobs live in /api/watcher/history."""
    out = []
    with _JOBS_LOCK:
        for jid, job in _JOBS.items():
            if job["status"] != "running":
                continue
            out.append({
                "job_id":      jid,
                "market":      job["market"],
                "max_deals":   job["max_deals"],
                "dry_run":     job["dry_run"],
                "status":      job["status"],
                "started_at":  job["started_at"],
                "finished_at": job["finished_at"],
                "deals_found": job["deals_found"],
                "log_tail":    list(job["log"])[-5:],
            })
    out.sort(key=lambda j: j["started_at"], reverse=True)
    return jsonify(out)


@app.route("/api/watcher/history")
def api_watcher_history():
    limit = int(request.args.get("limit", 50))
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT job_id, market, max_deals, dry_run, status,
                   deals_found, started_at, finished_at, exit_code
            FROM watcher_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return jsonify(_rows_to_list(rows))
    finally:
        conn.close()


@app.route("/api/watcher/stream/<job_id>")
def api_watcher_stream(job_id):
    job = _JOBS.get(job_id)
    if not job:
        return jsonify({"error": "unknown job"}), 404

    q: queue.Queue = queue.Queue()
    # Replay existing log, then subscribe for new lines
    backlog = list(job["log"])
    job["subscribers"].add(q)

    def gen():
        try:
            for line in backlog:
                yield f"data: {json.dumps({'line': line})}\n\n"
            # If already finished by the time we connect, send done and exit
            if job["status"] != "running":
                yield f"data: {json.dumps({'_done': True, 'status': job['status'], 'deals_found': job['deals_found']})}\n\n"
                return
            while True:
                try:
                    item = q.get(timeout=15)
                except queue.Empty:
                    yield ": keepalive\n\n"
                    continue
                if isinstance(item, dict) and item.get("_done"):
                    yield f"data: {json.dumps(item)}\n\n"
                    return
                yield f"data: {json.dumps({'line': item})}\n\n"
        finally:
            job["subscribers"].discard(q)

    return Response(stream_with_context(gen()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Comps Pipeline ────────────────────────────────────────────────────────────
#
# Runs comps_pipeline.run_comps_pipeline() in a background thread.
# Progress is streamed to the browser via Server-Sent Events, emitting
# structured step + progress messages so the frontend can drive a visual stepper.

_COMPS_JOBS: dict = {}
_COMPS_JOBS_LOCK = threading.Lock()

_COMPS_STEPS = [
    {"id": "geocode",  "pct_end": 10},
    {"id": "discover", "pct_end": 18},
    {"id": "process",  "pct_end": 90},
    {"id": "excel",    "pct_end": 100},
]


def _pct_to_step(pct: float) -> str:
    for s in _COMPS_STEPS:
        if pct <= s["pct_end"]:
            return s["id"]
    return "excel"


def _make_comps_progress_cb(job_id: str):
    last_step: list = [None]

    def cb(pct, msg):
        job = _COMPS_JOBS.get(job_id)
        if not job:
            return
        step_id = _pct_to_step(pct if pct is not None else 0)
        msgs = []
        if last_step[0] and last_step[0] != step_id:
            msgs.append({"type": "step", "step": last_step[0], "status": "done", "msg": ""})
        if step_id != last_step[0]:
            last_step[0] = step_id
        msgs.append({"type": "step", "step": step_id, "status": "active", "msg": msg})
        msgs.append({"type": "progress", "pct": pct, "msg": msg})
        for m in msgs:
            job["log"].append(m)
            for q in list(job["subscribers"]):
                try:
                    q.put_nowait(m)
                except Exception:
                    pass
    return cb


def _broadcast_comps(job_id: str, msg: dict):
    job = _COMPS_JOBS.get(job_id)
    if not job:
        return
    job["log"].append(msg)
    for q in list(job["subscribers"]):
        try:
            q.put_nowait(msg)
        except Exception:
            pass


@app.route("/api/comps/run", methods=["POST"])
def api_comps_run():
    body = request.get_json(silent=True) or {}
    location = (body.get("location") or "").strip()
    if not location:
        return jsonify({"error": "location required"}), 400

    try:
        radius_miles = float(body.get("radius_miles") or 5)
    except (ValueError, TypeError):
        radius_miles = 5.0

    acres        = float(body["acres"])        if body.get("acres")        else None
    asking_price = float(body["asking_price"]) if body.get("asking_price") else None
    crexi_url    = (body.get("crexi_url") or "").strip() or ""

    api_keys = {
        "google":    os.environ.get("GOOGLE_PLACES_API_KEY", ""),
        "firecrawl": comps_pipeline._get_env("FIRECRAWL_API_KEY"),
        "anthropic": comps_pipeline._get_env("ANTHROPIC_API_KEY"),
    }
    missing = [k for k, v in api_keys.items() if not v]
    if missing:
        return jsonify({"error": f"Missing API keys: {', '.join(missing)}"}), 400

    job_id     = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    stop_event = threading.Event()

    safe_loc    = "".join(c if c.isalnum() or c in " _-" else "_" for c in location)[:40].strip().replace(" ", "_")
    ts          = datetime.now().strftime("%b-%d-%y")
    output_path = os.path.join(PROJECT_DIR, "output", f"comps_{safe_loc}_{ts}.xlsx")
    os.makedirs(os.path.join(PROJECT_DIR, "output"), exist_ok=True)

    job = {
        "status":      "running",
        "started_at":  started_at,
        "finished_at": None,
        "output_path": None,
        "error":       None,
        "stop_event":  stop_event,
        "log":         deque(maxlen=500),
        "subscribers": set(),
    }
    with _COMPS_JOBS_LOCK:
        _COMPS_JOBS[job_id] = job

    def run():
        try:
            from comps_pipeline import run_comps_pipeline
            progress_cb = _make_comps_progress_cb(job_id)
            run_comps_pipeline(
                location=location,
                radius_miles=radius_miles,
                output_path=output_path,
                api_keys=api_keys,
                progress_cb=progress_cb,
                stop_flag=lambda: stop_event.is_set(),
                acres=acres,
                asking_price=asking_price,
                crexi_url=crexi_url,
            )
            job["output_path"] = output_path
            job["status"]      = "success"
            _broadcast_comps(job_id, {"type": "step", "step": "excel", "status": "done", "msg": "Report ready"})
        except Exception as exc:
            job["status"] = "stopped" if stop_event.is_set() else "error"
            job["error"]  = str(exc)
        finally:
            job["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            _broadcast_comps(job_id, {
                "_done":       True,
                "status":      job["status"],
                "output_path": job.get("output_path"),
                "error":       job.get("error"),
            })

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id, "started_at": started_at})


@app.route("/api/comps/stop/<job_id>", methods=["POST"])
def api_comps_stop(job_id):
    job = _COMPS_JOBS.get(job_id)
    if not job:
        return jsonify({"error": "unknown job"}), 404
    job["stop_event"].set()
    job["status"] = "stopped"
    return jsonify({"ok": True})


@app.route("/api/comps/stream/<job_id>")
def api_comps_stream(job_id):
    job = _COMPS_JOBS.get(job_id)
    if not job:
        return jsonify({"error": "unknown job"}), 404

    q: queue.Queue = queue.Queue()
    backlog = list(job["log"])
    job["subscribers"].add(q)

    def gen():
        try:
            for item in backlog:
                yield f"data: {json.dumps(item)}\n\n"
            if job["status"] != "running":
                yield f"data: {json.dumps({'_done': True, 'status': job['status'], 'output_path': job.get('output_path'), 'error': job.get('error')})}\n\n"
                return
            while True:
                try:
                    item = q.get(timeout=15)
                except queue.Empty:
                    yield ": keepalive\n\n"
                    continue
                if isinstance(item, dict) and item.get("_done"):
                    yield f"data: {json.dumps(item)}\n\n"
                    return
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            job["subscribers"].discard(q)

    return Response(stream_with_context(gen()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/comps/download/<job_id>")
def api_comps_download(job_id):
    job = _COMPS_JOBS.get(job_id)
    if not job or not job.get("output_path"):
        abort(404, description="Report not found")
    path = job["output_path"]
    if not os.path.exists(path):
        abort(404, description=f"File not found: {path}")
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    print("Storage Intel dashboard starting at http://localhost:5000")
    app.run(host="127.0.0.1", port=5000, debug=False)
