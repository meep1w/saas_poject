# migrate_add_username.py
import os, sqlite3

db = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")
path = db.split("///")[-1] if "///" in db else "saas.db"

con = sqlite3.connect(path)
cur = con.cursor()

def has_col(table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

if not has_col("user_access", "username"):
    cur.execute("ALTER TABLE user_access ADD COLUMN username TEXT")

con.commit()
con.close()
print("OK: username")
