"""
导出结果的持久化逻辑。
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from config_manager import DATA_DIR

logger = logging.getLogger("douyin_rpa")


class ExportPersistenceError(RuntimeError):
    def __init__(self, message: str, csv_path: Path):
        super().__init__(message)
        self.csv_path = csv_path


def _quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _ensure_sqlite_columns(connection: sqlite3.Connection, table: str, dataframe: pd.DataFrame) -> None:
    existing_columns = {
        row[1]
        for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    }
    missing_columns = [column for column in dataframe.columns if column not in existing_columns]

    for column in missing_columns:
        connection.execute(
            f'ALTER TABLE {_quote_identifier(table)} ADD COLUMN {_quote_identifier(column)} TEXT'
        )
    if missing_columns:
        connection.commit()


def _safe_filename_part(value: str, fallback: str) -> str:
    cleaned = "".join(char if (char.isalnum() or char in "._-") else "_" for char in (value or "").strip())
    cleaned = cleaned.strip("._-")
    return cleaned or fallback


def build_export_filename(
    *,
    portal_type: str,
    account_name: str,
    task_metadata: dict | None,
    now: datetime | None = None,
) -> str:
    task_metadata = task_metadata or {}
    now = now or datetime.now()
    scene_part = _safe_filename_part(task_metadata.get("scene_id", ""), "scene")
    account_part = _safe_filename_part(account_name, "account")
    task_part = _safe_filename_part(task_metadata.get("task_id", ""), now.strftime("%Y%m%d%H%M%S"))
    start_date = (task_metadata.get("target_start_date") or "").replace("-", "")
    end_date = (task_metadata.get("target_end_date") or "").replace("-", "")
    if start_date and end_date:
        date_part = start_date if start_date == end_date else f"{start_date}-{end_date}"
    else:
        date_part = now.strftime("%Y%m%d")

    return f"live_{portal_type}_{scene_part}_{account_part}_{date_part}_{task_part}.csv"


def persist_exported_dataframe(
    dataframe: pd.DataFrame,
    *,
    portal_type: str,
    account_name: str,
    config: dict,
    task_metadata: dict | None = None,
) -> tuple[pd.DataFrame, Path]:
    enriched = dataframe.copy()
    crawl_time = datetime.now().isoformat()
    task_metadata = task_metadata or {}
    enriched["抓取账号"] = account_name
    enriched["抓取入口"] = portal_type
    enriched["抓取时间"] = crawl_time
    enriched["抓取场景"] = task_metadata.get("scene_name", "")
    enriched["抓取日期模式"] = task_metadata.get("date_mode", "")
    enriched["目标开始日期"] = task_metadata.get("target_start_date", "")
    enriched["目标结束日期"] = task_metadata.get("target_end_date", "")
    enriched["任务ID"] = task_metadata.get("task_id", "")
    enriched["_crawl_time"] = crawl_time
    enriched["_portal_type"] = portal_type
    enriched["_account_name"] = account_name
    enriched["_scene_id"] = task_metadata.get("scene_id", "")
    enriched["_task_id"] = task_metadata.get("task_id", "")

    preferred_columns = [
        "抓取账号",
        "抓取入口",
        "抓取时间",
        "抓取场景",
        "抓取日期模式",
        "目标开始日期",
        "目标结束日期",
        "任务ID",
    ]
    remaining_columns = [
        column_name for column_name in enriched.columns if column_name not in preferred_columns
    ]
    enriched = enriched[preferred_columns + remaining_columns]

    csv_path = DATA_DIR / build_export_filename(
        portal_type=portal_type,
        account_name=account_name,
        task_metadata=task_metadata,
    )
    enriched.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"CSV 已保存: {csv_path}")

    try:
        _append_to_sqlite(enriched, config)
    except Exception as exc:
        raise ExportPersistenceError(
            f"CSV 已保存: {csv_path}；SQLite 写入失败: {exc}",
            csv_path,
        ) from exc
    return enriched, csv_path


def _append_to_sqlite(dataframe: pd.DataFrame, config: dict) -> None:
    db_path = config.get("sqlite_db", str(DATA_DIR / "douyin_compass.db"))
    table = config.get("sqlite_table", "rpa_douyin_launch_live")

    try:
        with sqlite3.connect(db_path) as connection:
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
                (table,),
            ).fetchone()
            if table_exists:
                _ensure_sqlite_columns(connection, table, dataframe)
            dataframe.to_sql(table, connection, if_exists="append", index=False)
        logger.info(f"SQLite 已保存: {db_path}")
    except Exception as exc:
        logger.error(f"SQLite 写入失败: {exc}")
        raise RuntimeError(str(exc)) from exc
