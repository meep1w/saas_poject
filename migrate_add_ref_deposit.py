# migrate_add_ref_deposit.py
import re, sqlite3
from app.settings import settings

db_url = settings.DATABASE_URL
m = re.match(r"sqlite\+aiosqlite:///(/?\.?.*)", db_url)
db_path = m.group(1) if m else "./saas.db"
if db_path.startswith("./"):
    db_path = db_path[2:]
print("DB path:", db_path)

con = sqlite3.connect(db_path)
cur = con.cursor()

def col_exists(table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

added = False
if not col_exists("tenants", "ref_link"):
    cur.execute("ALTER TABLE tenants ADD COLUMN ref_link VARCHAR(512)")
    print("Added: tenants.ref_link"); added = True
if not col_exists("tenants", "deposit_link"):
    cur.execute("ALTER TABLE tenants ADD COLUMN deposit_link VARCHAR(512)")
    print("Added: tenants.deposit_link"); added = True

con.commit(); con.close()
print("Migration done." if added else "No changes needed.")
