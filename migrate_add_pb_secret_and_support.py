# migrate_add_pb_secret_and_support.py
import os, sqlite3, secrets

DB = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")
path = DB.split("///")[-1] if "///" in DB else "saas.db"

con = sqlite3.connect(path)
cur = con.cursor()

def has_col(table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

added = False

if not has_col("tenants", "pb_secret"):
    cur.execute("ALTER TABLE tenants ADD COLUMN pb_secret VARCHAR(64)")
    added = True

if not has_col("tenants", "support_url"):
    cur.execute("ALTER TABLE tenants ADD COLUMN support_url VARCHAR(255)")
    added = True

# сгенерим секреты, где пусто
cur.execute("SELECT id, pb_secret FROM tenants")
for tid, sec in cur.fetchall():
    if not sec:
        cur.execute("UPDATE tenants SET pb_secret=? WHERE id=?", (secrets.token_hex(16), tid))

con.commit()
con.close()
print("OK: pb_secret/support_url")
