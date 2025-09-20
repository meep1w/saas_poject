# migrate_params.py
import os, sqlite3

db = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")
path = db.split("///")[-1] if "///" in db else "saas.db"

con = sqlite3.connect(path)
cur = con.cursor()

def has_col(table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

added = False
if not has_col("tenants", "check_subscription"):
    cur.execute("ALTER TABLE tenants ADD COLUMN check_subscription BOOLEAN DEFAULT 1")
    added = True
if not has_col("tenants", "check_deposit"):
    cur.execute("ALTER TABLE tenants ADD COLUMN check_deposit BOOLEAN DEFAULT 1")
    added = True
if not has_col("tenants", "min_deposit_usd"):
    cur.execute("ALTER TABLE tenants ADD COLUMN min_deposit_usd REAL DEFAULT 10.0")
    added = True
if not has_col("tenants", "platinum_threshold_usd"):
    cur.execute("ALTER TABLE tenants ADD COLUMN platinum_threshold_usd REAL DEFAULT 500.0")
    added = True

con.commit(); con.close()
print("OK: params" if added else "No changes")
