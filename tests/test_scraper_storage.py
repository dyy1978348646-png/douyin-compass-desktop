import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scraper_storage import _append_to_sqlite


class AppendToSqliteTests(unittest.TestCase):
    def test_auto_adds_missing_columns_before_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            with sqlite3.connect(db_path) as connection:
                connection.execute('CREATE TABLE "demo" ("抓取账号" TEXT)')
                connection.commit()

            dataframe = pd.DataFrame(
                [
                    {
                        "抓取账号": "达人首页",
                        "流量来源": "整体",
                        "short_video_leads_amount": 248279.33,
                    }
                ]
            )
            config = {"sqlite_db": str(db_path), "sqlite_table": "demo"}

            _append_to_sqlite(dataframe, config)

            with sqlite3.connect(db_path) as connection:
                columns = [
                    row[1]
                    for row in connection.execute('PRAGMA table_info("demo")').fetchall()
                ]
                rows = connection.execute(
                    'SELECT "抓取账号", "流量来源", "short_video_leads_amount" FROM "demo"'
                ).fetchall()

            self.assertIn("流量来源", columns)
            self.assertIn("short_video_leads_amount", columns)
            self.assertEqual(rows, [("达人首页", "整体", "248279.33")])


if __name__ == "__main__":
    unittest.main()
