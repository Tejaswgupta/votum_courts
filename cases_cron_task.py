import argparse
import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from cron_jobs.task_email import send_smtp_email
from cron_jobs.task_notifications import send_fcm_notification
from cron_jobs.task_sms import send_sms_message
from dotenv import load_dotenv
from ecourts import hc_services
from ecourts.ecourts import EcourtsService
from ecourts.gujarat_hc import get_gujarat_case_details
from ecourts.gujarat_hc import \
    persist_orders_to_storage as gujarat_persist_orders_to_storage
from ecourts.NCLT import nclt_get_details
from ecourts.NCLT import \
    persist_orders_to_storage as nclt_persist_orders_to_storage

from supabase import Client, create_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DEFAULT_DAYS_BACK = int(os.getenv("HC_CRON_DAYS_BACK", "1") or 1)
DEFAULT_DAYS_AHEAD = int(os.getenv("HC_CRON_DAYS_AHEAD", "0") or 0)

VOTUM_NOTIFICATIONS_TABLE = "votum_notifications"
CRON_JOB_RUNS_TABLE = "cron_job_runs"
ECOURTS_UID = "3f91159bc5ba1090:in.gov.ecourts.eCourtsServices"

BENCH_KEYWORDS = (
    "principal",
    "new delhi",
    "delhi",
    "mumbai",
    "cuttack",
    "ahmedabad",
    "amaravati",
    "chandigarh",
    "kolkata",
    "jaipur",
    "bengaluru",
    "bangalore",
    "chennai",
    "guwahati",
    "hyderabad",
    "kochi",
    "indore",
    "allahabad",
    "prayagraj",
)


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _create_cron_job_run(supabase: Client, job_name: str, metadata: dict) -> str | None:
    try:
        payload = {"job_name": job_name, "status": "running", "metadata": metadata}
        result = supabase.table(CRON_JOB_RUNS_TABLE).insert(payload).execute()
        if result.data:
            return result.data[0].get("id")
    except Exception as exc:
        logger.warning("Failed to create cron job run: %s", exc)
    return None


def _finish_cron_job_run(
    supabase: Client,
    run_id: str | None,
    status: str,
    summary: dict | None = None,
    error: str | None = None,
) -> None:
    if not run_id:
        return
    payload: dict[str, Any] = {
        "status": status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    if summary is not None:
        payload["summary"] = summary
    if error:
        payload["error"] = error
    try:
        supabase.table(CRON_JOB_RUNS_TABLE).update(payload).eq("id", run_id).execute()
    except Exception as exc:
        logger.warning("Failed to update cron job run %s: %s", run_id, exc)


def _order_key(order: dict) -> str | None:
    if not isinstance(order, dict):
        return None
    description = (order.get("description") or "").strip().lower()
    date_value = (order.get("date") or "").strip()
    if description and date_value:
        return f"{date_value}|{description}"
    if description:
        return f"desc|{description}"
    if date_value:
        return f"date|{date_value}"
    return None


def _normalize_court_name(value: str | None) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_")


def _infer_court_key(case_record: dict) -> str:
    court_name_raw = case_record.get("court_name") or ""
    court_name = court_name_raw.lower()
    court_key = _normalize_court_name(court_name_raw)

    if "nclat" in court_name or court_key.startswith("nclat"):
        return "NCLAT"
    if "nclt" in court_name or court_key.startswith("nclt"):
        return "NCLT"
    if (
        "gujarat" in court_name and "high court" in court_name
        or court_key in {"guj_hc", "gujarat_hc", "gjhc", "gujarat_high_court"}
    ):
        return "GUJ_HC"
    if "high court" in court_name or court_key.endswith("hc"):
        return "HC"
    if "district" in court_name or court_key.endswith("dc"):
        return "DC"
    return "DC"


def _extract_case_year(case_record: dict) -> str | None:
    for value in (
        case_record.get("case_no"),
        case_record.get("registration_no"),
        case_record.get("cin_no"),
    ):
        if not value:
            continue
        match = re.search(r"(19|20)\d{2}", str(value))
        if match:
            return match.group(0)
    return None


def _extract_gujarat_case_parts(case_record: dict) -> tuple[str | None, str | None, str | None]:
    case_type = case_record.get("case_type")
    case_no = case_record.get("case_no")
    registration_no = case_record.get("registration_no")
    case_year = _extract_case_year(case_record)

    source = registration_no or case_no or ""
    if source:
        parts = [p for p in re.split(r"[/-]", str(source)) if p]
        if parts:
            if not case_type:
                case_type = parts[0]
            digits = [p for p in parts if p.isdigit()]
            if len(digits) >= 2:
                case_no = digits[-2]
                case_year = digits[-1]

    return case_type, case_no, case_year


def _infer_bench_name(case_record: dict) -> str | None:
    for value in (case_record.get("bench_name"), case_record.get("court_name")):
        if not value:
            continue
        normalized = str(value).lower()
        for bench in BENCH_KEYWORDS:
            if bench in normalized:
                return bench
    return None


def merge_orders(
    existing: list[dict] | None,
    incoming: list[dict] | None,
) -> tuple[list[dict], int, list[dict]]:
    existing_orders = existing if isinstance(existing, list) else []
    incoming_orders = incoming if isinstance(incoming, list) else []

    seen_keys: set[str] = set()
    merged: list[dict] = []
    added_orders: list[dict] = []

    for order in existing_orders:
        if not isinstance(order, dict):
            continue
        key = _order_key(order)
        if key:
            seen_keys.add(key)
        merged.append(order)

    added = 0
    for order in incoming_orders:
        if not isinstance(order, dict):
            continue
        key = _order_key(order)
        if key and key in seen_keys:
            continue
        if key:
            seen_keys.add(key)
        merged.append(order)
        added_orders.append(order)
        added += 1

    return merged, added, added_orders


def _persist_case_orders(
    orders: list[dict] | None,
    case_id: str | int | None,
    court_key: str | None = None,
) -> list[dict] | None:
    if not orders or case_id is None:
        return None

    try:
        if court_key == "NCLT":
            persist_fn = nclt_persist_orders_to_storage
        elif court_key == "GUJ_HC":
            persist_fn = gujarat_persist_orders_to_storage
        else:
            persist_fn = hc_services.persist_orders_to_storage

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            new_loop = asyncio.new_event_loop()
            try:
                return new_loop.run_until_complete(
                    persist_fn(orders, case_id=case_id)
                )
            finally:
                new_loop.close()

        return asyncio.run(persist_fn(orders, case_id=case_id))
    except Exception as exc:
        logger.warning(
            "Failed to persist order documents for case %s: %s",
            case_id,
            exc,
        )
        return None


def _map_added_orders(
    stored_orders: list[dict],
    added_orders_list: list[dict],
) -> list[dict]:
    stored_by_key: dict[str | None, dict] = {
        _order_key(order): order for order in stored_orders if _order_key(order)
    }
    updated_added_orders: list[dict] = []
    for order in added_orders_list:
        key = _order_key(order)
        if key and key in stored_by_key:
            updated_added_orders.append(stored_by_key[key])
        else:
            updated_added_orders.append(order)
    return updated_added_orders


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 3)]}..."


def should_sync_case(
    next_listing_date: Any,
    days_back: int,
    days_ahead: int,
    target_date: date | None = None,
    allow_missing_date: bool = False,
) -> bool:
    if not next_listing_date:
        return allow_missing_date
    try:
        if isinstance(next_listing_date, date):
            case_date = next_listing_date
        else:
            case_date = datetime.strptime(str(next_listing_date), "%Y-%m-%d").date()
    except Exception:
        return False

    if target_date:
        return case_date == target_date

    today = date.today()
    start_date = today - timedelta(days=days_back)
    # Exclude today from the default range to ensure we only sync after the listing date (n+1)
    # This prevents running too early on the listing date when data isn't ready.
    end_date = today + timedelta(days=days_ahead) - timedelta(days=1)
    return start_date <= case_date <= end_date


def fetch_case_details(
    case_record: dict,
    court_key: str,
    dc_service: EcourtsService | None = None,
) -> dict | None:
    cin_no = case_record.get("cin_no")

    if court_key == "GUJ_HC":
        case_type, case_no, case_year = _extract_gujarat_case_parts(case_record)
        if not (case_type and case_no and case_year):
            logger.info(
                "Skipping Gujarat HC case %s: missing case_type/case_no/case_year",
                case_record.get("id"),
            )
            return None
        return get_gujarat_case_details(case_type, case_no, case_year)

    if court_key == "NCLT":
        filing_no = case_record.get("filling_no") or cin_no
        bench = _infer_bench_name(case_record)
        if not (filing_no and bench):
            logger.info(
                "Skipping NCLT case %s: missing filing_no/bench",
                case_record.get("id"),
            )
            return None
        return nclt_get_details(bench, filing_no)

    if court_key == "NCLAT":
        filing_no = case_record.get("filling_no") or cin_no
        bench = _infer_bench_name(case_record)
        if not (filing_no and bench):
            logger.info(
                "Skipping NCLAT case %s: missing filing_no/bench",
                case_record.get("id"),
            )
            return None
        return nclat_get_details(filing_no, bench)

    if court_key == "DC":
        if not cin_no:
            return None
        if not dc_service:
            dc_service = EcourtsService("DC", ECOURTS_UID)
        return dc_service.get_by_cnr(cin_no)

    if not cin_no:
        return None
    return hc_services.hc_search_by_cnr(cin_no)


def fetch_cases(supabase: Client, limit: int | None = None) -> list[dict]:
    query = (
        supabase.table("votum_cases")
        .select(
            "id, cin_no, filling_no, next_listing_date, orders, case_no, case_type, "
            "registration_no, bench_name, court_name, petitioner, respondent, "
            "assigned_user_ids, reminder_contacts, workspace_id, ia_details"
        )
        .order("id")
    )
    if limit:
        query = query.limit(limit)
    response = query.execute()
    return response.data or []


def build_case_label(case_record: dict) -> str:
    case_no = case_record.get("case_no")
    cin_no = case_record.get("cin_no")
    court_name = case_record.get("court_name")
    parts = [p for p in [case_no or cin_no, court_name] if p]
    return " | ".join(parts) if parts else f"Case {case_record.get('id')}"


def build_event_message(event_type: str, case_record: dict, payload: dict) -> tuple[str, str]:
    case_label = build_case_label(case_record)
    if event_type == "next_date_updated":
        next_date = payload.get("next_listing_date") or "TBD"
        subject = f"Next hearing date updated: {case_label}"
        body = f"{case_label} has a new next hearing date: {next_date}."
        return subject, body

    order = payload.get("order") or {}
    order_date = order.get("date") or "Unknown date"
    description = order.get("description") or "New order added"
    subject = f"New order uploaded: {case_label}"
    body = f"{case_label} has a new order ({order_date}): {description}."
    return subject, body


def get_or_create_notification(
    supabase: Client,
    workspace_id: str,
    target_user_id: str,
    event_type: str,
    event_key: str,
    title: str,
    message: str,
    redirect_uri: str,
    metadata: dict,
    created_by_id: str | None = None,
) -> tuple[str | None, bool]:
    existing = (
        supabase.table(VOTUM_NOTIFICATIONS_TABLE)
        .select("id")
        .eq("workspace_id", workspace_id)
        .eq("target_user_id", target_user_id)
        .eq("type", "case")
        .eq("subtype", event_type)
        .contains("metadata", {"case_id": metadata.get("case_id"), "event_key": event_key})
        .limit(1)
        .execute()
    )
    if existing.data:
        return existing.data[0].get("id"), False

    insert_payload = {
        "type": "case",
        "subtype": event_type,
        "module": "case",
        "redirect_uri": redirect_uri,
        "title": title,
        "message": message,
        "workspace_id": workspace_id,
        "target_user_id": target_user_id,
        "created_by_id": created_by_id,
        "related_entity_type": "case",
        "is_obsolete": False,
        "metadata": metadata | {"event_key": event_key},
    }
    inserted = supabase.table(VOTUM_NOTIFICATIONS_TABLE).insert(insert_payload).execute()
    if inserted.data:
        return inserted.data[0].get("id"), True
    return None, False


def send_notifications_for_event(
    supabase: Client,
    case_record: dict,
    event_type: str,
    event_payload: dict,
    event_key: str,
) -> None:
    subject, body = build_event_message(event_type, case_record, event_payload)
    sms_body = _truncate(body, 150)
    workspace_id = case_record.get("workspace_id")
    case_id = case_record.get("id")
    if not workspace_id or case_id is None:
        return

    redirect_uri = f"/home/cases/{case_id}"
    metadata = {
        "case_id": str(case_id),
        "cin_no": case_record.get("cin_no"),
    }

    reminder_contacts = case_record.get("reminder_contacts") or []
    for contact in reminder_contacts:
        if not isinstance(contact, dict):
            continue
        contact_type = contact.get("type")
        contact_value = contact.get("value")
        if not contact_type or not contact_value:
            continue
        if contact_type == "email":
            success = send_smtp_email(contact_value, subject, body, f"<p>{body}</p>")
            if not success:
                logger.warning("Failed to send reminder email to %s", contact_value)
        elif contact_type == "phone":
            sms_result = send_sms_message(contact_value, body)
            if not sms_result.get("success"):
                logger.warning(
                    "Failed to send reminder SMS to %s: %s",
                    contact_value,
                    sms_result.get("error"),
                )

    assigned_user_ids = case_record.get("assigned_user_ids") or []
    assigned_user_emails: dict[str, str] = {}
    assigned_user_phones: dict[str, str] = {}
    if assigned_user_ids:
        try:
            user_rows = (
                supabase.table("votum_users")
                .select("id, email, phone_number")
                .in_("id", assigned_user_ids)
                .execute()
            )
            assigned_user_emails = {
                row.get("id"): row.get("email")
                for row in (user_rows.data or [])
                if row.get("id") and row.get("email")
            }
            assigned_user_phones = {
                row.get("id"): row.get("phone_number")
                for row in (user_rows.data or [])
                if row.get("id") and row.get("phone_number")
            }
        except Exception as exc:
            logger.warning("Failed to load assigned user emails: %s", exc)

    for user_id in assigned_user_ids:
        if not user_id:
            continue
        user_email = assigned_user_emails.get(user_id)
        if user_email:
            success = send_smtp_email(user_email, subject, body, f"<p>{body}</p>")
            if not success:
                logger.warning("Failed to send assigned user email to %s", user_email)
        user_phone = assigned_user_phones.get(user_id)
        if user_phone:
            sms_result = send_sms_message(user_phone, sms_body)
            if not sms_result.get("success"):
                logger.warning(
                    "Failed to send assigned user SMS to %s: %s",
                    user_phone,
                    sms_result.get("error"),
                )
        notification_id, created = get_or_create_notification(
            supabase,
            workspace_id,
            user_id,
            event_type,
            event_key,
            subject,
            body,
            redirect_uri,
            metadata,
        )
        if not notification_id or not created:
            continue
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                new_loop = asyncio.new_event_loop()
                try:
                    success = new_loop.run_until_complete(
                        send_fcm_notification(user_id, subject, body, supabase)
                    )
                finally:
                    new_loop.close()
            else:
                success = asyncio.run(
                    send_fcm_notification(user_id, subject, body, supabase)
                )
        except RuntimeError:
            success = False
        if not success:
            logger.warning("Failed to send push notification to user %s", user_id)


def sync_case_record(
    supabase: Client,
    case_record: dict,
    dry_run: bool = False,
    court_key: str | None = None,
    dc_service: EcourtsService | None = None,
) -> dict:
    case_id = case_record.get("id")
    cin_no = case_record.get("cin_no")
    if not court_key:
        court_key = _infer_court_key(case_record)
    if not cin_no and court_key in {"HC", "DC"}:
        logger.info("Skipping case %s: missing CIN", case_id)
        return {"case_id": case_id, "status": "skipped", "reason": "missing_cin"}

    try:
        hc_data = fetch_case_details(case_record, court_key, dc_service=dc_service)
    except Exception as exc:
        logger.warning("Failed to refresh case %s (%s): %s", case_id, court_key, exc)
        return {"case_id": case_id, "status": "error", "reason": str(exc)}

    if not hc_data:
        logger.info("No data returned for case %s (%s)", case_id, court_key)
        return {"case_id": case_id, "status": "empty"}

    next_listing_date = (
        hc_data.get("next_listing_date")
        or hc_data.get("next_hearing_date")
        or hc_data.get("next_hearing")
    )
    existing_next = case_record.get("next_listing_date")
    existing_orders = case_record.get("orders") or []
    incoming_orders = hc_data.get("orders") or []
    existing_ia_details = case_record.get("ia_details") or []
    incoming_ia_details = (
        hc_data.get("ia_details")
        or (hc_data.get("original_json") or {}).get("ia_details")
        or []
    )

    merged_orders, added_orders, added_orders_list = merge_orders(
        existing_orders, incoming_orders
    )

    update_payload: dict[str, Any] = {}
    if next_listing_date and next_listing_date != existing_next:
        update_payload["next_listing_date"] = next_listing_date
    if added_orders:
        update_payload["orders"] = merged_orders
    if court_key == "GUJ_HC" and incoming_ia_details != existing_ia_details:
        update_payload["ia_details"] = incoming_ia_details

    if not update_payload:
        return {
            "case_id": case_id,
            "status": "no_change",
            "added_orders": 0,
            "ia_details_updated": False,
        }

    if dry_run:
        logger.info("Dry run: would update case %s with %s", case_id, update_payload)
        return {
            "case_id": case_id,
            "status": "dry_run",
            "added_orders": added_orders,
            "ia_details_updated": bool(
                court_key == "GUJ_HC"
                and incoming_ia_details != existing_ia_details
            ),
        }

    if added_orders:
        stored_orders = _persist_case_orders(
            merged_orders,
            case_id,
            court_key=court_key,
        )
        if stored_orders:
            merged_orders = stored_orders
            added_orders_list = _map_added_orders(stored_orders, added_orders_list)
            update_payload["orders"] = merged_orders

    supabase.table("votum_cases").update(update_payload).eq("id", case_id).execute()

    if next_listing_date and next_listing_date != existing_next:
        event_key = f"next_date|{next_listing_date}"
        event_payload = {
            "case_id": case_id,
            "cin_no": cin_no,
            "next_listing_date": next_listing_date,
            "previous_next_listing_date": existing_next,
        }
        send_notifications_for_event(
            supabase, case_record, "next_date_updated", event_payload, event_key
        )

    if added_orders_list:
        for order in added_orders_list:
            order_key = _order_key(order)
            if not order_key:
                continue
            event_key = f"order|{order_key}"
            event_payload = {
                "case_id": case_id,
                "cin_no": cin_no,
                "order": order,
            }
            send_notifications_for_event(
                supabase, case_record, "order_added", event_payload, event_key
            )

    return {
        "case_id": case_id,
        "status": "updated",
        "added_orders": added_orders,
        "ia_details_updated": bool(
            court_key == "GUJ_HC"
            and incoming_ia_details != existing_ia_details
        ),
    }


def run_case_sync(
    supabase: Client | None = None,
    limit: int | None = None,
    days_back: int | None = DEFAULT_DAYS_BACK,
    days_ahead: int | None = DEFAULT_DAYS_AHEAD,
    dry_run: bool = False,
    target_date: date | None = None,
) -> list[dict]:
    if days_back is None:
        days_back = DEFAULT_DAYS_BACK
    if days_ahead is None:
        days_ahead = DEFAULT_DAYS_AHEAD
    if supabase is None:
        supabase = get_supabase_client()
    cases = fetch_cases(supabase, limit=limit)

    results: list[dict] = []
    dc_service: EcourtsService | None = None
    for case_record in cases:
        court_key = _infer_court_key(case_record)
        allow_missing_date = court_key in {"NCLT", "NCLAT", "DC", "GUJ_HC"}
        if not should_sync_case(
            case_record.get("next_listing_date"),
            days_back=days_back,
            days_ahead=days_ahead,
            target_date=target_date,
            allow_missing_date=allow_missing_date,
        ):
            results.append(
                {
                    "case_id": case_record.get("id"),
                    "status": "skipped",
                    "reason": "outside_window",
                }
            )
            continue
        if court_key == "DC" and dc_service is None:
            dc_service = EcourtsService("DC", ECOURTS_UID)
        results.append(
            sync_case_record(
                supabase,
                case_record,
                dry_run=dry_run,
                court_key=court_key,
                dc_service=dc_service,
            )
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh HC next hearing dates and orders in Supabase."
    )
    parser.add_argument("--limit", type=int, default=None, help="Max cases to sync.")
    parser.add_argument(
        "--days-back",
        type=int,
        default=DEFAULT_DAYS_BACK,
        help="Sync cases with next hearing within N days back (default 1).",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=DEFAULT_DAYS_AHEAD,
        help="Sync cases with next hearing within N days ahead (default 0).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log updates only.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    run_id = _create_cron_job_run(
        supabase,
        "hc_case_sync",
        {
            "limit": args.limit,
            "days_back": args.days_back,
            "days_ahead": args.days_ahead,
            "dry_run": args.dry_run,
        },
    )
    run_status = "failed"
    run_error: str | None = None
    run_summary: dict | None = None

    try:
        results = run_case_sync(
            supabase=supabase,
            limit=args.limit,
            days_back=args.days_back,
            days_ahead=args.days_ahead,
            dry_run=args.dry_run,
        )
        updated = sum(1 for r in results if r.get("status") == "updated")
        skipped = sum(1 for r in results if r.get("status") == "skipped")
        errors = sum(1 for r in results if r.get("status") == "error")
        run_summary = {
            "total": len(results),
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }
        run_status = "success"
        logger.info("Sync complete. Updated %s cases.", updated)
    except Exception as exc:
        run_error = str(exc)
        logger.exception("HC case sync failed")
        raise
    finally:
        _finish_cron_job_run(
            supabase,
            run_id,
            run_status,
            summary=run_summary,
            error=run_error,
        )


if __name__ == "__main__":
    main()
