from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time

from src.models import TokenEntry


def _get_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _get_primary_key_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall() if row[5] > 0}


def _migrate_old_schema(conn: sqlite3.Connection):
    cols = _get_columns(conn, "request_log")
    if "provider" not in cols:
        print("[DB] Migrating: adding 'provider' column to request_log")
        conn.execute(
            "ALTER TABLE request_log ADD COLUMN provider TEXT NOT NULL DEFAULT 'default'"
        )
        if _get_columns(conn, "request_log") & {"token", "ts"}:
            conn.execute(
                "UPDATE request_log SET provider = 'default' WHERE provider = '' OR provider IS NULL"
            )
        conn.commit()

    pk_cols = _get_primary_key_columns(conn, "request_log")
    if pk_cols == {"token", "ts"}:
        print(
            "[DB] Migrating: rebuilding request_log with PRIMARY KEY (token, provider, ts)"
        )
        conn.execute(
            "CREATE TABLE request_log_new ("
            "  token TEXT NOT NULL,"
            "  provider TEXT NOT NULL DEFAULT 'default',"
            "  ts INTEGER NOT NULL,"
            "  count INTEGER NOT NULL DEFAULT 1,"
            "  PRIMARY KEY (token, provider, ts)"
            ")"
        )
        conn.execute(
            "INSERT INTO request_log_new (token, provider, ts, count) "
            "SELECT token, provider, ts, count FROM request_log"
        )
        conn.execute("DROP TABLE request_log")
        conn.execute("ALTER TABLE request_log_new RENAME TO request_log")
        conn.commit()
        print("[DB] Migration complete: request_log primary key updated")


def _migrate_pool_json(
    conn: sqlite3.Connection, pool_file: str, admin_tokens: list[str]
):
    if not os.path.exists(pool_file):
        return

    existing_count = conn.execute("SELECT COUNT(*) FROM token_pool").fetchone()[0]
    if existing_count > 0:
        backup = pool_file + ".bak"
        if not os.path.exists(backup):
            shutil.move(pool_file, backup)
            print(f"[DB] pool.json migrated to DB, backed up to {backup}")
        return

    with open(pool_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_tokens = data.get("tokens", [])
    now = int(time.time())
    imported = 0
    for entry in raw_tokens:
        if isinstance(entry, str):
            t = entry
            provs = ["*"]
        elif isinstance(entry, dict):
            t = entry.get("token", "")
            provs = entry.get("providers", ["*"])
        else:
            continue
        if not t:
            continue
        is_admin = t in admin_tokens
        conn.execute(
            "INSERT OR IGNORE INTO token_pool (token, providers, is_admin, enabled, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (t, json.dumps(provs), 1 if is_admin else 0, 1, now),
        )
        imported += 1

    conn.commit()
    backup = pool_file + ".bak"
    if not os.path.exists(backup):
        shutil.move(pool_file, backup)
        print(f"[DB] Imported {imported} tokens from pool.json, backed up to {backup}")


def init_db(
    usage_db: str,
    pool_file: str = "",
    admin_tokens: list[str] | None = None,
    pool_count: int = 5,
) -> tuple[sqlite3.Connection, list[str]]:
    db_dir = os.path.dirname(usage_db)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(usage_db)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS request_log ("
        "  token TEXT NOT NULL,"
        "  provider TEXT NOT NULL DEFAULT 'default',"
        "  ts    INTEGER NOT NULL,"
        "  count INTEGER NOT NULL DEFAULT 1,"
        "  PRIMARY KEY (token, provider, ts)"
        ")"
    )
    _migrate_old_schema(conn)

    conn.execute(
        "CREATE TABLE IF NOT EXISTS token_pool ("
        "  token       TEXT PRIMARY KEY,"
        "  providers   TEXT NOT NULL DEFAULT '[\"*\"]',"
        "  is_admin    INTEGER NOT NULL DEFAULT 0,"
        "  enabled     INTEGER NOT NULL DEFAULT 1,"
        "  created_at  INTEGER NOT NULL"
        ")"
    )

    conn.execute("UPDATE token_pool SET enabled = 1 WHERE enabled = 0")
    conn.commit()

    generated_admins: list[str] = []

    has_any_admin = conn.execute(
        "SELECT COUNT(*) FROM token_pool WHERE is_admin = 1"
    ).fetchone()[0]

    admin_tokens = admin_tokens or []
    if not admin_tokens and has_any_admin == 0:
        from src.pool import make_token

        new_admin = make_token()
        admin_tokens = [new_admin]
        generated_admins.append(new_admin)

    _migrate_pool_json(conn, pool_file, admin_tokens)

    if admin_tokens:
        placeholders = ",".join("?" for _ in admin_tokens)
        conn.execute(
            f"UPDATE token_pool SET is_admin = 1 WHERE token IN ({placeholders})",
            admin_tokens,
        )
        for t in admin_tokens:
            exists = conn.execute(
                "SELECT 1 FROM token_pool WHERE token = ?", (t,)
            ).fetchone()
            if not exists:
                now = int(time.time())
                conn.execute(
                    "INSERT OR IGNORE INTO token_pool (token, providers, is_admin, enabled, created_at) "
                    "VALUES (?, ?, 1, 1, ?)",
                    (t, '["*"]', now),
                )
        conn.commit()

    current_count = conn.execute("SELECT COUNT(*) FROM token_pool").fetchone()[0]
    if current_count < pool_count:
        need = pool_count - current_count
        from src.pool import make_token

        now = int(time.time())
        for _ in range(need):
            t = make_token()
            conn.execute(
                "INSERT OR IGNORE INTO token_pool (token, providers, is_admin, enabled, created_at) "
                "VALUES (?, ?, 0, 1, ?)",
                (t, '["*"]', now),
            )
        conn.commit()
        print(f"[DB] Added {need} tokens to pool, now {pool_count} total")

    conn.execute(
        "DELETE FROM request_log WHERE ts < ?", (int(time.time()) - 35 * 86400,)
    )
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM token_pool").fetchone()[0]
    print(f"[DB] {usage_db} initialized, {total} tokens in pool")
    return conn, generated_admins


def record_usage(conn: sqlite3.Connection, token: str, provider_name: str):
    now = int(time.time())
    hour_ts = now - (now % 3600)
    conn.execute(
        "INSERT INTO request_log (token, provider, ts, count) VALUES (?, ?, ?, 1) "
        "ON CONFLICT(token, provider, ts) DO UPDATE SET count = count + 1",
        (token, provider_name, hour_ts),
    )
    conn.commit()


def get_all_tokens(conn: sqlite3.Connection) -> list[TokenEntry]:
    cur = conn.execute(
        "SELECT token, providers, is_admin, enabled, created_at FROM token_pool "
        "ORDER BY created_at DESC"
    )
    return [TokenEntry.from_db_row(row) for row in cur.fetchall()]


def get_token(conn: sqlite3.Connection, token: str) -> TokenEntry | None:
    cur = conn.execute(
        "SELECT token, providers, is_admin, enabled, created_at FROM token_pool WHERE token = ?",
        (token,),
    )
    row = cur.fetchone()
    return TokenEntry.from_db_row(row) if row else None


def is_admin_token(conn: sqlite3.Connection, token: str) -> bool:
    row = conn.execute(
        "SELECT is_admin, enabled FROM token_pool WHERE token = ?", (token,)
    ).fetchone()
    if not row:
        return False
    return bool(row[0]) and bool(row[1])


def add_token(
    conn: sqlite3.Connection,
    token: str,
    providers: list[str],
    is_admin: bool = False,
) -> TokenEntry:
    entry = TokenEntry(token=token, providers=providers, is_admin=is_admin)
    conn.execute(
        "INSERT OR IGNORE INTO token_pool (token, providers, is_admin, enabled, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        entry.to_db_row(),
    )
    conn.commit()
    return entry


def update_token(
    conn: sqlite3.Connection,
    token: str,
    providers: list[str] | None = None,
    is_admin: bool | None = None,
    enabled: bool | None = None,
) -> TokenEntry | None:
    entry = get_token(conn, token)
    if not entry:
        return None
    if providers is not None:
        entry.providers = providers
    if is_admin is not None:
        entry.is_admin = is_admin
    if enabled is not None:
        entry.enabled = enabled
    conn.execute(
        "UPDATE token_pool SET providers=?, is_admin=?, enabled=? WHERE token=?",
        (
            json.dumps(entry.providers),
            1 if entry.is_admin else 0,
            1 if entry.enabled else 0,
            token,
        ),
    )
    conn.commit()
    return entry


def delete_token(conn: sqlite3.Connection, token: str) -> bool:
    cur = conn.execute("DELETE FROM token_pool WHERE token = ?", (token,))
    conn.commit()
    return cur.rowcount > 0
