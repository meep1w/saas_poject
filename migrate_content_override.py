# migrate_content_override.py
import os, sqlite3, re

db_url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")
m = re.match(r"sqlite\+aiosqlite:///(/?\.?.*)", db_url)
path = (m.group(1) if m else "./saas.db").lstrip("./")

con = sqlite3.connect(path)
cur = con.cursor()

# проверим, существует ли таблица
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='content_override'")
exists = cur.fetchone() is not None

if not exists:
    cur.execute("""
        CREATE TABLE content_override (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id INTEGER NOT NULL,
            lang VARCHAR(5) NOT NULL,
            screen VARCHAR(32) NOT NULL,
            title VARCHAR(512),
            primary_btn_text VARCHAR(128),
            photo_file_id VARCHAR(256),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_content_override UNIQUE (tenant_id, lang, screen)
        );
    """)
    print("Created: content_override")
else:
    print("OK: content_override exists")

con.commit()
con.close()
