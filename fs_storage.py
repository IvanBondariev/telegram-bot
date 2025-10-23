import os
import json
from typing import Any, Dict, Tuple

BASE_DIR = os.path.dirname(__file__)
STORAGE_DIR = os.path.join(BASE_DIR, "storage")
PENDING_DIR = os.path.join(STORAGE_DIR, "pending")
APPROVED_DIR = os.path.join(STORAGE_DIR, "approved")
REJECTED_DIR = os.path.join(STORAGE_DIR, "rejected")
import shutil


def ensure_dirs() -> None:
    os.makedirs(PENDING_DIR, exist_ok=True)
    os.makedirs(APPROVED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)


def _approved_subdir(approved_at_iso: str | None) -> str:
    # approved_at: 'YYYY-MM-DDTHH:MM:SS.ssssss'
    if not approved_at_iso:
        return APPROVED_DIR
    try:
        y = approved_at_iso[0:4]
        m = approved_at_iso[5:7]
        subdir = os.path.join(APPROVED_DIR, f"{y}-{m}")
        os.makedirs(subdir, exist_ok=True)
        return subdir
    except Exception:
        return APPROVED_DIR


def _file_path(dir_path: str, profit_id: int) -> str:
    return os.path.join(dir_path, f"profit_{profit_id}.json")


def _row_to_dict(row: Tuple[Any, ...]) -> Dict[str, Any]:
    return {
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "first_name": row[3],
        "original_amount": row[4],
        "final_amount": row[5],
        "note": row[6],
        "status": row[7],
        "created_at": row[8],
        "approved_at": row[9],
        "approver_id": row[10],
    }


def save_pending_profit(row: Tuple[Any, ...]) -> str:
    ensure_dirs()
    data = _row_to_dict(row)
    path = _file_path(PENDING_DIR, data["id"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def save_approved_profit(row: Tuple[Any, ...]) -> str:
    ensure_dirs()
    data = _row_to_dict(row)
    # удалить из pending, если был
    pending_path = _file_path(PENDING_DIR, data["id"])
    if os.path.exists(pending_path):
        try:
            os.remove(pending_path)
        except Exception:
            pass
    # запись в approved/YYYY-MM
    subdir = _approved_subdir(data.get("approved_at"))
    path = _file_path(subdir, data["id"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def save_rejected_profit(row: Tuple[Any, ...]) -> str:
    ensure_dirs()
    data = _row_to_dict(row)
    # удалить из pending, если был
    pending_path = _file_path(PENDING_DIR, data["id"])
    if os.path.exists(pending_path):
        try:
            os.remove(pending_path)
        except Exception:
            pass
    # запись в rejected
    path = _file_path(REJECTED_DIR, data["id"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def purge_storage() -> None:
    """Полностью очистить хранилище файлов (pending/approved/rejected)."""
    try:
        shutil.rmtree(STORAGE_DIR, ignore_errors=True)
    except Exception:
        pass
    ensure_dirs()


def purge_approved_and_pending() -> None:
    """Очистить approved (включая подкаталоги месяцев) и pending."""
    for path in (APPROVED_DIR, PENDING_DIR):
        try:
            shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass
    ensure_dirs()


def remove_files_for_profit_id(profit_id: int) -> None:
    """Удалить файлы заявки из pending и approved по её id."""
    ensure_dirs()
    # Удаляем из pending
    try:
        pending_path = _file_path(PENDING_DIR, profit_id)
        if os.path.exists(pending_path):
            os.remove(pending_path)
    except Exception:
        pass
    # Удаляем из approved во всех месячных подкаталогах
    try:
        if os.path.exists(APPROVED_DIR):
            for name in os.listdir(APPROVED_DIR):
                subdir = os.path.join(APPROVED_DIR, name)
                if os.path.isdir(subdir):
                    approved_path = _file_path(subdir, profit_id)
                    if os.path.exists(approved_path):
                        try:
                            os.remove(approved_path)
                        except Exception:
                            pass
    except Exception:
        pass