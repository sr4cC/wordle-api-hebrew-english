
import os
import sqlite3
from pathlib import Path

DB_PATH = os.getenv("WORDDB_PATH", "words.db")
WORDS_FILE = "words.txt" 

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    return conn

def seed():
    conn = get_conn()
    cur = conn.cursor()
    if not Path(WORDS_FILE).exists():
        print(f"Words file not found: {WORDS_FILE}")
        return
    with open(WORDS_FILE, "r", encoding="utf-8") as f:
        words = [w.strip().lower() for w in f if w.strip()]
    inserted = 0
    for w in words:
        try:
            cur.execute("INSERT OR IGNORE INTO words(word) VALUES(?);", (w,))
            inserted += cur.rowcount
        except Exception as e:
            print("Error inserting", w, e)
    conn.commit()
    print(f"Done. Inserted (or ignored duplicates): {len(words)} lines processed.")
    conn.close()

if __name__ == "__main__":
    seed()
