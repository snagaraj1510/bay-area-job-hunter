"""
SQLite database for persistence.

Tables:
  jobs:
    - id TEXT PRIMARY KEY (hash of title+company+location)
    - title TEXT
    - company TEXT
    - location TEXT
    - description TEXT
    - job_url TEXT
    - source TEXT (indeed/linkedin/etc)
    - date_posted TEXT (ISO date)
    - date_scraped TEXT (ISO date)
    - comp_min REAL
    - comp_max REAL
    - comp_interval TEXT
    - score INTEGER
    - score_breakdown TEXT (JSON)
    - status TEXT DEFAULT 'new' (new/sent/applied/rejected/saved)
    - notes TEXT

  scrape_runs:
    - id INTEGER PRIMARY KEY
    - timestamp TEXT
    - total_scraped INTEGER
    - total_after_dedup INTEGER
    - total_after_filter INTEGER
    - total_sent INTEGER
    - errors TEXT (JSON)
"""

import sqlite3
import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path


def generate_job_id(title: str, company: str, location: str) -> str:
    """Creates a deterministic SHA-256 hash from normalized title, company, and location."""
    normalized = f"{title.strip().lower()}|{company.strip().lower()}|{location.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class JobStorage:
    """Manages the SQLite database for job hunting persistence."""

    JOBS_COLUMNS = [
        "id",
        "title",
        "company",
        "location",
        "description",
        "job_url",
        "source",
        "date_posted",
        "date_scraped",
        "comp_min",
        "comp_max",
        "comp_interval",
        "score",
        "score_breakdown",
        "status",
        "notes",
    ]

    def __init__(self, db_path="data/jobs.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._create_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _create_tables(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    company TEXT,
                    location TEXT,
                    description TEXT,
                    job_url TEXT,
                    source TEXT,
                    date_posted TEXT,
                    date_scraped TEXT,
                    comp_min REAL,
                    comp_max REAL,
                    comp_interval TEXT,
                    score INTEGER,
                    score_breakdown TEXT,
                    status TEXT DEFAULT 'new',
                    notes TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT,
                    total_scraped INTEGER,
                    total_after_dedup INTEGER,
                    total_after_filter INTEGER,
                    total_sent INTEGER,
                    errors TEXT
                )
            """)
            conn.commit()

    def save_jobs(self, jobs_df: pd.DataFrame):
        """Upserts jobs from a pandas DataFrame using INSERT OR REPLACE."""
        if jobs_df.empty:
            return

        # Only keep columns that exist in both the DataFrame and our schema
        cols = [c for c in self.JOBS_COLUMNS if c in jobs_df.columns]
        df = jobs_df[cols].copy()

        # Serialize score_breakdown to JSON string if it's not already a string
        if "score_breakdown" in df.columns:
            df["score_breakdown"] = df["score_breakdown"].apply(
                lambda v: json.dumps(v) if not isinstance(v, str) and v is not None else v
            )

        placeholders = ", ".join("?" * len(cols))
        col_names = ", ".join(cols)
        sql = f"INSERT OR REPLACE INTO jobs ({col_names}) VALUES ({placeholders})"

        with self._connect() as conn:
            conn.executemany(sql, df.itertuples(index=False, name=None))
            conn.commit()

    def get_unsent_jobs(self, min_score=0) -> pd.DataFrame:
        """Returns jobs with status='new' and score >= min_score."""
        sql = """
            SELECT * FROM jobs
            WHERE status = 'new' AND (score IS NULL OR score >= ?)
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(min_score,))
        return df

    def mark_jobs_sent(self, job_ids: list):
        """Updates status to 'sent' for the given job IDs."""
        if not job_ids:
            return
        placeholders = ", ".join("?" * len(job_ids))
        sql = f"UPDATE jobs SET status = 'sent' WHERE id IN ({placeholders})"
        with self._connect() as conn:
            conn.execute(sql, job_ids)
            conn.commit()

    def mark_job_status(self, job_id: str, status: str):
        """Updates status for a single job."""
        sql = "UPDATE jobs SET status = ? WHERE id = ?"
        with self._connect() as conn:
            conn.execute(sql, (status, job_id))
            conn.commit()

    def is_seen(self, job_id: str) -> bool:
        """Returns True if job_id exists in DB and was scraped within the last 14 days."""
        cutoff = (datetime.utcnow() - timedelta(days=14)).date().isoformat()
        sql = "SELECT 1 FROM jobs WHERE id = ? AND date_scraped >= ? LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(sql, (job_id, cutoff)).fetchone()
        return row is not None

    def get_seen_ids(self, days=14) -> set:
        """Returns a set of job IDs seen (scraped) in the last N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
        sql = "SELECT id FROM jobs WHERE date_scraped >= ?"
        with self._connect() as conn:
            rows = conn.execute(sql, (cutoff,)).fetchall()
        return {row["id"] for row in rows}

    def log_scrape_run(self, stats: dict):
        """Inserts a row into scrape_runs with the provided stats."""
        sql = """
            INSERT INTO scrape_runs
                (timestamp, total_scraped, total_after_dedup, total_after_filter, total_sent, errors)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, (
                stats.get("timestamp", datetime.utcnow().isoformat()),
                stats.get("total_scraped"),
                stats.get("total_after_dedup"),
                stats.get("total_after_filter"),
                stats.get("total_sent"),
                json.dumps(stats.get("errors")) if stats.get("errors") is not None else None,
            ))
            conn.commit()

    def get_stats(self) -> dict:
        """Returns summary stats: total jobs, jobs by status, and recent scrape runs."""
        with self._connect() as conn:
            total_jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

            status_rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM jobs GROUP BY status"
            ).fetchall()
            jobs_by_status = {row["status"]: row["count"] for row in status_rows}

            recent_runs_df = pd.read_sql_query(
                "SELECT * FROM scrape_runs ORDER BY id DESC LIMIT 10",
                conn,
            )

        return {
            "total_jobs": total_jobs,
            "jobs_by_status": jobs_by_status,
            "recent_scrape_runs": recent_runs_df.to_dict(orient="records"),
        }

    def export_jobs(self, min_score=0) -> pd.DataFrame:
        """Returns all jobs with score >= min_score as a DataFrame."""
        sql = "SELECT * FROM jobs WHERE score IS NULL OR score >= ?"
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(min_score,))
        return df

    def purge_old(self, days=30):
        """Deletes jobs whose date_scraped is older than N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
        sql = "DELETE FROM jobs WHERE date_scraped < ?"
        with self._connect() as conn:
            conn.execute(sql, (cutoff,))
            conn.commit()
