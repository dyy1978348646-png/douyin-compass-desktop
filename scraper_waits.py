"""
Playwright 等待辅助，尽量用显式等待替换硬编码 sleep。
"""

from __future__ import annotations

import time
from collections.abc import Callable


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float,
    interval_seconds: float = 0.5,
    on_poll: Callable[[], None] | None = None,
    timeout_message: str = "等待超时",
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if predicate():
            return
        if on_poll:
            on_poll()
        time.sleep(interval_seconds)

    raise TimeoutError(timeout_message)


def wait_for_page_ready(page, timeout_ms: int = 10000) -> None:
    for state in ("domcontentloaded", "networkidle"):
        try:
            page.wait_for_load_state(state, timeout=timeout_ms)
        except Exception:
            continue
