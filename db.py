import os
import sqlite3
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "bot.db")


def _connect():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS profits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                original_amount REAL NOT NULL,
                final_amount REAL,
                note TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                approved_at TEXT,
                approver_id INTEGER
            )
            """
        )
        # Таблица пользователей: фиксируем дату первого взаимодействия и последнюю активность
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                first_seen TEXT NOT NULL,
                last_seen TEXT
            )
            """
        )
        # Индексы для ускорения запросов
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_profits_status_approved_at
            ON profits(status, approved_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_profits_user_id
            ON profits(user_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_profits_username
            ON profits(username)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_last_seen
            ON users(last_seen)
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_profit_request(user_id: int, username: str | None, first_name: str | None,
                          amount: float, note: str | None) -> int:
    conn = _connect()
    try:
        now = datetime.utcnow().isoformat()
        cur = conn.execute(
            """
            INSERT INTO profits (user_id, username, first_name, original_amount, final_amount, note, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (user_id, username, first_name, amount, amount, note, now),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_profit(profit_id: int):
    conn = _connect()
    try:
        cur = conn.execute("SELECT * FROM profits WHERE id = ?", (profit_id,))
        row = cur.fetchone()
        return row
    finally:
        conn.close()


def update_final_amount(profit_id: int, new_amount: float):
    conn = _connect()
    try:
        conn.execute(
            "UPDATE profits SET final_amount = ? WHERE id = ?",
            (new_amount, profit_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_status(profit_id: int, status: str, approver_id: int | None = None):
    conn = _connect()
    try:
        approved_at = datetime.utcnow().isoformat() if status == "approved" else None
        conn.execute(
            "UPDATE profits SET status = ?, approver_id = ?, approved_at = ? WHERE id = ?",
            (status, approver_id, approved_at, profit_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_approved_profits_between(start_iso: str | None, end_iso: str | None):
    conn = _connect()
    try:
        base_sql = "SELECT user_id, username, first_name, final_amount, approved_at FROM profits WHERE status = 'approved'"
        params = []
        if start_iso:
            base_sql += " AND approved_at >= ?"
            params.append(start_iso)
        if end_iso:
            base_sql += " AND approved_at <= ?"
            params.append(end_iso)
        cur = conn.execute(base_sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def get_all_profits():
    conn = _connect()
    try:
        cur = conn.execute("SELECT * FROM profits")
        return cur.fetchall()
    finally:
        conn.close()


def reset_all_to_rejected() -> int:
    conn = _connect()
    try:
        cur = conn.execute(
            "UPDATE profits SET status = 'rejected', approved_at = NULL, approver_id = NULL WHERE status != 'rejected'"
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


def delete_all_profits():
    conn = _connect()
    try:
        conn.execute("DELETE FROM profits")
        conn.commit()
    finally:
        conn.close()


# --- Учёт вступления пользователей в группу ---

def get_profits_by_user(user_id: int):
    conn = _connect()
    try:
        cur = conn.execute("SELECT * FROM profits WHERE user_id = ?", (user_id,))
        return cur.fetchall()
    finally:
        conn.close()


def reset_user_to_rejected(user_id: int) -> int:
    conn = _connect()
    try:
        cur = conn.execute(
            "UPDATE profits SET status = 'rejected', approved_at = NULL, approver_id = NULL WHERE user_id = ? AND status != 'rejected'",
            (user_id,),
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


def get_user_ids_by_username(username: str):
    conn = _connect()
    try:
        cur = conn.execute("SELECT DISTINCT user_id FROM profits WHERE username = ?", (username,))
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def ensure_user_seen(user_id: int, username: str | None, first_name: str | None) -> None:
    conn = _connect()
    try:
        now = datetime.utcnow().isoformat()
        cur = conn.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if cur.fetchone():
            conn.execute(
                "UPDATE users SET username = ?, first_name = ?, last_seen = ? WHERE user_id = ?",
                (username, first_name, now, user_id),
            )
        else:
            conn.execute(
                "INSERT INTO users (user_id, username, first_name, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
                (user_id, username, first_name, now, now),
            )
        conn.commit()
    finally:
        conn.close()


def get_user_first_seen(user_id: int) -> str | None:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT first_seen FROM users WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()