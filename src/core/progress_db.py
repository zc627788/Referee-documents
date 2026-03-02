"""SQLite 断点续传 + 动态角色发现"""
import sqlite3
import json
import threading
from pathlib import Path
from typing import List, Dict, Optional


class ProgressDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            c = sqlite3.connect(self.db_path)
            c.row_factory = sqlite3.Row
            self._local.conn = c
        return self._local.conn

    def _init_db(self):
        self._conn().executescript("""
            CREATE TABLE IF NOT EXISTS progress (
                file_name   TEXT NOT NULL,
                row_index   INTEGER NOT NULL,
                status      TEXT NOT NULL,
                method      TEXT,
                confidence  REAL,
                result_json TEXT,
                PRIMARY KEY (file_name, row_index)
            );
            CREATE TABLE IF NOT EXISTS discovered_roles (
                role_text  TEXT PRIMARY KEY,
                count      INTEGER DEFAULT 1,
                sample     TEXT,
                confirmed  INTEGER DEFAULT 0,
                first_seen TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_status ON progress(status);
        """)
        self._conn().commit()

    def is_processed(self, file_name: str, row_index: int) -> bool:
        row = self._conn().execute(
            "SELECT 1 FROM progress WHERE file_name=? AND row_index=?",
            (file_name, row_index)
        ).fetchone()
        return row is not None

    def mark_done(self, file_name: str, row_index: int,
                  method: str, confidence: float, result: dict):
        self._conn().execute(
            """INSERT OR REPLACE INTO progress
               (file_name, row_index, status, method, confidence, result_json)
               VALUES (?,?,'done',?,?,?)""",
            (file_name, row_index, method, confidence,
             json.dumps(result, ensure_ascii=False))
        )

    def mark_pending_ai(self, file_name: str, row_index: int, text: str):
        self._conn().execute(
            """INSERT OR REPLACE INTO progress
               (file_name, row_index, status, result_json)
               VALUES (?,?,'pending_ai',?)""",
            (file_name, row_index, text)
        )

    def mark_no_signature(self, file_name: str, row_index: int):
        self._conn().execute(
            """INSERT OR REPLACE INTO progress
               (file_name, row_index, status)
               VALUES (?,?,'no_signature')""",
            (file_name, row_index)
        )

    def commit(self):
        self._conn().commit()

    def get_pending_ai(self) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT file_name, row_index, result_json FROM progress WHERE status='pending_ai'"
        ).fetchall()
        return [dict(r) for r in rows]

    def log_discovered_role(self, role: str, context: str):
        conn = self._conn()
        exists = conn.execute(
            "SELECT 1 FROM discovered_roles WHERE role_text=?", (role,)
        ).fetchone()
        if exists:
            conn.execute("UPDATE discovered_roles SET count=count+1 WHERE role_text=?", (role,))
        else:
            conn.execute(
                "INSERT INTO discovered_roles (role_text, sample) VALUES (?,?)",
                (role, context[:120])
            )

    def get_discovered_roles(self) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT role_text, count, sample FROM discovered_roles "
            "WHERE confirmed=0 ORDER BY count DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def confirm_role(self, role: str, roles_json_path: str):
        """确认角色并写入 roles.json"""
        path = Path(roles_json_path)
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        names = [r['name'] for r in data['roles']]
        if role not in names:
            max_pri = max(r['priority'] for r in data['roles'])
            data['roles'].append({"name": role, "priority": max_pri})
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        self._conn().execute(
            "UPDATE discovered_roles SET confirmed=1 WHERE role_text=?", (role,)
        )
        self._conn().commit()

    def get_stats(self) -> Dict:
        stats = {}
        for s in ('done', 'pending_ai', 'no_signature'):
            r = self._conn().execute(
                "SELECT COUNT(*) as c FROM progress WHERE status=?", (s,)
            ).fetchone()
            stats[s] = r['c']
        stats['total'] = sum(stats.values())
        return stats

    def close(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
