import re, sqlite3
from app.settings import settings

db_url = settings.DATABASE_URL
m = re.match(r"sqlite\+aiosqlite:///(/?\.?.*)", db_url)
db_path = m.group(1) if m else "./saas.db"
if db_path.startswith("./"): db_path = db_path[2:]

con = sqlite3.connect(db_path)
cur = con.cursor()
cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_user_access_click_id ON user_access(click_id)")
con.commit(); con.close()
print("OK: unique index on user_access.click_id")
