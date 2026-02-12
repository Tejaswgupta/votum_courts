"""
Background sync entrypoint used by APScheduler (see backend/scheduler.py).

The scheduler historically expected a `run_case_sync(...)` function; this module provides
that wrapper and delegates actual work to backend/cron_jobs/case_hearing_sync.py.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

from cron_jobs.case_hearing_sync import run_hearing_day_case_updates

logger = logging.getLogger(__name__)


def run_case_sync(
    workspace_id: Optional[str] = None,
    user_id: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    target_date: Optional[date] = None,
) -> list[Dict[str, Any]]:
    """
    Hearing-date sync:
    - fetch next hearing date
    - update orders

    Note: `workspace_id` and `user_id` are accepted for backward compatibility with
    earlier versions of this job, but are not used yet.
    """
    try:
        if isinstance(target_date, datetime):
            day = target_date.date()
        else:
            day = target_date
        result = run_hearing_day_case_updates(target_date=day, dry_run=dry_run, limit=limit)
        updated = int(((result or {}).get("summary") or {}).get("updated") or 0)
        status = "updated" if updated > 0 else "noop"
        # scheduler.py expects a list of per-case results; keep a compact list that still
        # allows accurate "updated" counting.
        return ([{"status": "updated"}] * updated) or [{"status": status, "summary": result}]
    except Exception as exc:
        logger.warning("run_case_sync failed: %s", exc, exc_info=True)
        return [{"status": "error", "error": str(exc)}]
