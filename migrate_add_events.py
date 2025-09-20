# migrate_add_events.py
import re, sqlite3
from app.settings import settings

db_url = settings.DATABASE_URL
m = re.match(r"sqlite\+aiosqlite:///(/?\.?.*)", db_url)
db_path = m.group(1) if m else "./saas.db"
if db_path.startswith("./"): db_path = db_path[2:]

con = sqlite3.connect(db_path)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER,
    user_id BIGINT,
    click_id VARCHAR(64),
    kind VARCHAR(16),
    amount REAL,
    raw_qs TEXT,
    created_at DATETIME DEFAULT (datetime('now'))
)
""")
cur.execute("CREATE INDEX IF NOT EXISTS ix_events_tenant ON events(tenant_id)")
cur.execute("CREATE INDEX IF NOT EXISTS ix_events_click ON events(click_id)")
con.commit(); con.close()
print("OK: events")
