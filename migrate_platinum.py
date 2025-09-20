# migrate_platinum.py
import os, sqlite3

db = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")
path = db.split("///")[-1] if "///" in db else "saas.db"

con = sqlite3.connect(path)
cur = con.cursor()

def has_col(table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

added_cols = []

if not has_col("user_access", "is_platinum"):
    cur.execute("ALTER TABLE user_access ADD COLUMN is_platinum BOOLEAN DEFAULT 0")
    added_cols.append("is_platinum")

if not has_col("user_access", "platinum_shown"):
    cur.execute("ALTER TABLE user_access ADD COLUMN platinum_shown BOOLEAN DEFAULT 0")
    added_cols.append("platinum_shown")

# Нормализуем старые строки (где значения NULL)
cur.execute("UPDATE user_access SET is_platinum=0 WHERE is_platinum IS NULL")
cur.execute("UPDATE user_access SET platinum_shown=0 WHERE platinum_shown IS NULL")

con.commit()
con.close()

if added_cols:
    print("Added columns:", ", ".join(added_cols))
else:
    print("No changes")
