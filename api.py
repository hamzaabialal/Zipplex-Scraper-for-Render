import os
import uuid
import sqlite3
import json
import threading
from datetime import datetime
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from scraper import Zipplex, find_item, HTTP_COOKIES

app = FastAPI(title="Zipplex Scraper API")

DB_PATH = "jobs.db"

# ---------------------------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id   TEXT PRIMARY KEY,
            status   TEXT NOT NULL DEFAULT 'pending',
            result   TEXT,
            error    TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def job_set_status(job_id: str, status: str, result=None, error=None):
    with get_conn() as conn:
        conn.execute(
            "UPDATE jobs SET status=?, result=?, error=? WHERE job_id=?",
            (status, json.dumps(result) if result is not None else None, error, job_id)
        )

def job_create(job_id: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, status, created_at) VALUES (?, 'pending', ?)",
            (job_id, datetime.utcnow().isoformat())
        )

def job_get_and_delete(job_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row:
            conn.execute("DELETE FROM jobs WHERE job_id=?", (job_id,))
        return dict(row) if row else None

init_db()

# ---------------------------------------------------------------------------
# SCRAPER RUNNER (background thread)
# ---------------------------------------------------------------------------

def run_scrape(job_id: str, keyword: str, year: int, options: dict):
    try:
        job_set_status(job_id, "running")
        zipplex = Zipplex(cookies=HTTP_COOKIES)
        result = find_item(zipplex, keyword, year, options)
        if result:
            job_set_status(job_id, "done", result=result)
        else:
            job_set_status(job_id, "failed", error="No results found for the given keyword.")
    except Exception as e:
        job_set_status(job_id, "failed", error=str(e))

# ---------------------------------------------------------------------------
# REQUEST MODELS
# ---------------------------------------------------------------------------

class ScrapeRequest(BaseModel):
    keyword: str
    year: int = 2020
    options: dict = {}

# ---------------------------------------------------------------------------
# ENDPOINTS
# ---------------------------------------------------------------------------

@app.post("/scrape")
def start_scrape(req: ScrapeRequest):
    """
    Trigger a scrape job.
    Returns a job_id you can use to poll /results/{job_id}.
    """
    job_id = str(uuid.uuid4())
    job_create(job_id)

    thread = threading.Thread(
        target=run_scrape,
        args=(job_id, req.keyword, req.year, req.options),
        daemon=True
    )
    thread.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/results/{job_id}")
def get_results(job_id: str):
    """
    Poll this endpoint for results.
    - status=pending/running  → scraper still working, poll again
    - status=done             → result returned AND deleted from DB
    - status=failed           → error message returned AND deleted from DB
    """
    job = job_get_and_delete(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found. It may have already been retrieved.")

    if job["status"] in ("pending", "running"):
        # Put it back — not done yet
        with get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO jobs (job_id, status, result, error, created_at) VALUES (?,?,?,?,?)",
                (job["job_id"], job["status"], job["result"], job["error"], job["created_at"])
            )
        return {"job_id": job_id, "status": job["status"], "message": "Still running, try again in a few seconds."}

    # Done or failed — already deleted from DB
    response = {"job_id": job_id, "status": job["status"]}
    if job["result"]:
        response["result"] = json.loads(job["result"])
    if job["error"]:
        response["error"] = job["error"]

    return response


@app.get("/health")
def health():
    return {"status": "ok"}
