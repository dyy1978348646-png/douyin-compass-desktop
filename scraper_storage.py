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


def persist_exported_dataframe(
    dataframe: pd.DataFrame,
    *,
    portal_type: str,
    account_name: str,
    config: dict,
) -> tuple[pd.DataFrame, Path]:
    enriched = dataframe.copy()
    enriched["_crawl_time"] = datetime.now().isoformat()
    enriched["_portal_type"] = portal_type
    enriched["_account_name"] = account_name

    date_str = datetime.now().strftime("%Y%m%d")
    safe_name = "".join(char for char in account_name if char.isalnum() or char in "._-")
    name_tag = f"_{safe_name}" if safe_name else ""
    csv_path = DATA_DIR / f"live_{portal_type}{name_tag}_{date_str}.csv"
    enriched.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"CSV 已保存: {csv_path}")

    _append_to_sqlite(enriched, config)
    return enriched, csv_path


def _append_to_sqlite(dataframe: pd.DataFrame, config: dict) -> None:
    db_path = config.get("sqlite_db", str(DATA_DIR / "douyin_compass.db"))
    table = config.get("sqlite_table", "rpa_douyin_launch_live")

    try:
        connection = sqlite3.connect(db_path)
        dataframe.to_sql(table, connection, if_exists="append", index=False)
        connection.close()
        logger.info(f"SQLite 已保存: {db_path}")
    except Exception as exc:
        logger.error(f"SQLite 写入失败: {exc}")
