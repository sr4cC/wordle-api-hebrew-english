# main.py
import os
import random
import sqlite3
from datetime import datetime, time, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

DB_PATH = os.getenv("WORDDB_PATH", "words.db")

app = FastAPI(title="Daily Word API (FastAPI + SQLite + APScheduler)")


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    with conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL UNIQUE,
            used INTEGER NOT NULL DEFAULT 0,
            used_at TEXT
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_words (
            date TEXT PRIMARY KEY,
            word_id INTEGER NOT NULL,
            FOREIGN KEY (word_id) REFERENCES words(id)
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS words_hebrew (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL UNIQUE,
            used INTEGER NOT NULL DEFAULT 0,
            used_at TEXT
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_words_hebrew (
            date TEXT PRIMARY KEY,
            word_id INTEGER NOT NULL,
            FOREIGN KEY (word_id) REFERENCES words_hebrew(id)
        );
        """)
    conn.close()


init_db()


class WordOut(BaseModel):
    id: int
    word: str
    used: bool
    used_at: Optional[str] = None

class WordOutHeb(BaseModel):
    id: int
    word: str
    used: bool
    used_at: Optional[str] = None


def pick_and_mark_unused() -> WordOut:
    """
    Atomically pick a random unused word and mark it used with timestamp.
    Uses a SQLite transaction with immediate locking to avoid races.
    Raises HTTPException if no unused words remain.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        cur = conn.execute("SELECT id, word FROM words WHERE used = 0 ORDER BY RANDOM() LIMIT 1;")
        row = cur.fetchone()
        if row is None:
            conn.execute("ROLLBACK;")
            raise HTTPException(status_code=404, detail="No unused words available")
        now = datetime.utcnow().date().isoformat()
        conn.execute("UPDATE words SET used = 1, used_at = ? WHERE id = ?;", (now, row["id"]))
        conn.execute("COMMIT;")
        return WordOut(id=row["id"], word=row["word"], used=True, used_at=now)
    except:
        try:
            conn.execute("ROLLBACK;")
        except:
            pass
        raise
    finally:
        conn.close()


def pick_and_mark_unused_hebrew() -> WordOutHeb:
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        cur = conn.execute("SELECT id, word FROM words_hebrew WHERE used = 0 ORDER BY RANDOM() LIMIT 1;")
        row = cur.fetchone()
        if row is None:
            conn.execute("ROLLBACK;")
            raise HTTPException(status_code=404, detail="No unused Hebrew words available")

        now = datetime.utcnow().date().isoformat()
        conn.execute("UPDATE words_hebrew SET used = 1, used_at = ? WHERE id = ?;", (now, row["id"]))
        conn.execute("COMMIT;")
        return WordOutHeb(id=row["id"], word=row["word"], used=True, used_at=now)
    except:
        try: conn.execute("ROLLBACK;")
        except: pass
        raise
    finally:
        conn.close()


def get_random_word_from_all() -> WordOut:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT id, word, used, used_at FROM words ORDER BY RANDOM() LIMIT 1;")
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="No words in database")
        return WordOut(
            id=row["id"],
            word=row["word"],
            used=bool(row["used"]),
            used_at=row["used_at"]
        )
    finally:
        conn.close()

def get_random_word_hebrew() -> WordOutHeb:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT id, word, used, used_at FROM words_hebrew ORDER BY RANDOM() LIMIT 1;")
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="No Hebrew words in database")
        return WordOutHeb(
            id=row["id"], word=row["word"],
            used=bool(row["used"]), used_at=row["used_at"]
        )
    finally:
        conn.close()


def get_daily_word_for_date(date_str: str) -> Optional[WordOut]:
    """Return the saved daily word for the given date (if exists)."""
    conn = get_conn()
    try:
        cur = conn.execute("""
            SELECT w.id, w.word, w.used, w.used_at
            FROM daily_words d
            JOIN words w ON w.id = d.word_id
            WHERE d.date = ?;
        """, (date_str,))
        row = cur.fetchone()
        if row:
            return WordOut(
                id=row["id"],
                word=row["word"],
                used=bool(row["used"]),
                used_at=row["used_at"]
            )
        return None
    finally:
        conn.close()

def get_daily_word_hebrew_for_date(date_str: str) -> Optional[WordOutHeb]:
    conn = get_conn()
    try:
        cur = conn.execute("""
            SELECT w.id, w.word, w.used, w.used_at
            FROM daily_words_hebrew d
            JOIN words_hebrew w ON w.id = d.word_id
            WHERE d.date = ?;
        """, (date_str,))
        row = cur.fetchone()
        if row:
            return WordOutHeb(
                id=row["id"], word=row["word"],
                used=bool(row["used"]), used_at=row["used_at"]
            )
        return None
    finally:
        conn.close()


def save_daily_word(date_str: str, word: WordOut):
    """Save the word of the day persistently."""
    conn = get_conn()
    try:
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO daily_words (date, word_id)
                VALUES (?, ?);
            """, (date_str, word.id))
    finally:
        conn.close()

def save_daily_word_hebrew(date_str: str, word: WordOutHeb):
    conn = get_conn()
    try:
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO daily_words_hebrew (date, word_id)
                VALUES (?, ?);
            """, (date_str, word.id))
    finally:
        conn.close()

def word_exists_english(word: str) -> bool:
    word = word.strip().lower()
    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM words WHERE word = ? LIMIT 1;", (word,))
        return cur.fetchone() is not None
    finally:
        conn.close()

def word_exists_hebrew(word: str) -> bool:
    word = word.strip()
    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM words_hebrew WHERE word = ? LIMIT 1;", (word,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def get_today_word() -> Optional[WordOut]:
    today_str = datetime.utcnow().date().isoformat()
    return get_daily_word_for_date(today_str)

def get_today_word_hebrew() -> Optional[WordOutHeb]:
    today_str = datetime.utcnow().date().isoformat()
    return get_daily_word_hebrew_for_date(today_str)

def choose_daily_word_job():
    today_str = datetime.utcnow().date().isoformat()
    existing = get_daily_word_for_date(today_str)
    if existing:
        print(f"[{datetime.utcnow().isoformat()}] Word for {today_str} already chosen: {existing.word}")
        return
    try:
        chosen = pick_and_mark_unused()
        save_daily_word(today_str, chosen)
        print(f"[{datetime.utcnow().isoformat()}] Chosen daily word for {today_str}: {chosen.word}")
    except HTTPException as e:
        print(f"[{datetime.utcnow().isoformat()}] Daily job: {e.detail}")
    except Exception as exc:
        print(f"[{datetime.utcnow().isoformat()}] Daily job error: {exc}")

def choose_daily_word_hebrew_job():
    today_str = datetime.utcnow().date().isoformat()
    existing = get_daily_word_hebrew_for_date(today_str)
    if existing:
        print(f"[{datetime.utcnow().isoformat()}] Hebrew word for {today_str} already chosen: {existing.word}")
        return
    try:
        chosen = pick_and_mark_unused_hebrew()
        save_daily_word_hebrew(today_str, chosen)
        print(f"[{datetime.utcnow().isoformat()}] Chosen Hebrew daily word for {today_str}: {chosen.word}")
    except HTTPException as e:
        print(f"[{datetime.utcnow().isoformat()}] Hebrew daily job: {e.detail}")
    except Exception as exc:
        print(f"[{datetime.utcnow().isoformat()}] Hebrew daily job error: {exc}")


def schedule_midnight_job(scheduler: BackgroundScheduler):
    scheduler.add_job(choose_daily_word_job, CronTrigger(hour=0, minute=0))
    scheduler.add_job(choose_daily_word_hebrew_job, CronTrigger(hour=0, minute=0))
    scheduler.start()


scheduler = BackgroundScheduler()
schedule_midnight_job(scheduler)

@app.get("/is-exists-english")
def api_word_exists_english(word: str) -> bool:
    return word_exists_english(word)

@app.get("/is-exists-hebrew")
def api_word_exists_hebrew(word: str) -> bool:
    return word_exists_hebrew(word)


@app.get("/random", response_model=WordOut)
def api_random_word():
    """
    Return a random word from the entire list (no state change).
    """
    return get_random_word_from_all()

@app.get("/random-hebrew", response_model=WordOutHeb)
def api_random_hebrew():
    return get_random_word_hebrew()


@app.get("/today", response_model=Optional[WordOut])
def api_today_word():
    """
    Return today's chosen word if already selected (UTC-day).
    If scheduler hasn't run yet today, this returns null.
    """
    return get_today_word()

@app.get("/today-hebrew", response_model=Optional[WordOutHeb])
def api_today_hebrew():
    return get_today_word_hebrew()

@app.get("/stats")
def api_stats():
    """
    Returns total counts and the last 10 days' chosen words.
    """
    conn = get_conn()
    try:
        cur = conn.execute("SELECT COUNT(*) as total, SUM(used) as used FROM words;")
        row = cur.fetchone()
        total = row["total"] or 0
        used = row["used"] or 0
        unused = total - used

        cur = conn.execute("""
            SELECT d.date, w.word
            FROM daily_words d
            JOIN words w ON w.id = d.word_id
            ORDER BY d.date DESC
            LIMIT 10;
        """)
        history = [{"date": r["date"], "word": r["word"]} for r in cur.fetchall()]

        return {
            "total": total,
            "used": used,
            "unused": unused,
            "last_10_days": history
        }
    finally:
        conn.close()

def import_hebrew_words(file_path="words_hebrew.txt"):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    conn = get_conn()
    try:
        with conn:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    word = line.strip()
                    if word:
                        conn.execute("INSERT OR IGNORE INTO words_hebrew (word) VALUES (?);", (word,))
    finally:
        conn.close()


def import_words_from_file(file_path="words.txt"):
    """
    Reads words from a text file (one per line) and inserts them into the database.
    Ignores duplicates.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    conn = get_conn()
    try:
        with conn:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    word = line.strip().lower()
                    if word: 
                        try:
                            conn.execute("INSERT OR IGNORE INTO words (word) VALUES (?);", (word,))
                        except Exception as e:
                            print(f"Failed to insert word '{word}': {e}")
        print("Words import completed.")
    finally:
        conn.close()

@app.post("/choose-today", response_model=WordOut)
def api_force_today_choice():
    today_str = datetime.utcnow().date().isoformat()
    existing = get_daily_word_for_date(today_str)
    if existing:
        return existing
    chosen = pick_and_mark_unused()
    save_daily_word(today_str, chosen)
    return chosen

@app.post("/choose-today-hebrew", response_model=WordOutHeb)
def api_force_today_choice_hebrew():
    today_str = datetime.utcnow().date().isoformat()
    existing = get_daily_word_hebrew_for_date(today_str)
    if existing:
        return existing
    chosen = pick_and_mark_unused_hebrew()
    save_daily_word_hebrew(today_str, chosen)
    return chosen


import_words_from_file()
import_hebrew_words()
