import json
import logging
import re
import time
import hashlib
import uuid
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import ddddocr
import httpx
import requests
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)
from fastapi import APIRouter, Form, HTTPException

from cron_jobs.task_email import send_smtp_email
from cron_jobs.task_notifications import send_fcm_notification
from cron_jobs.task_sms import send_sms_message
from cron_jobs.utils.extract_case import find_case_entries, save_screenshots
from supabase_client import get_supabase_client

try:
    from .order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    from order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)

# Router for side-effects/cron usage if needed
router = APIRouter()

CRON_JOB_RUNS_TABLE = "cron_job_runs"
VOTUM_NOTIFICATIONS_TABLE = "votum_notifications"

BASE_URL = "https://gujarathc-casestatus.nic.in/gujarathc"
CASE_TYPE_URL = f"{BASE_URL}/GetCaseTypeDataOnLoad"
CAPTCHA_URL = f"{BASE_URL}/CaptchaServlet?ct=S&tm={{}}"
DATA_URL = f"{BASE_URL}/GetData"

# Cause List Constants
CAUSE_LIST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-GB,en;q=0.5",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded",
    "pragma": "no-cache",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "sec-gpc": "1",
    "upgrade-insecure-requests": "1",
    "Referer": "https://gujarathc-casestatus.nic.in/gujarathc/",
}

CAUSE_LIST_HOME_URL = "https://gujarathc-casestatus.nic.in/gujarathc/"
CAUSE_LIST_PRINT_URL = "https://gujarathc-casestatus.nic.in/gujarathc/printBoardNew"

def parse_listing_date(date_str: Optional[str]) -> datetime:
    if not date_str:
        return datetime.now() + timedelta(days=1)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError("Invalid listing_date format. Use YYYY-MM-DD or DD/MM/YYYY.")

def fetch_cause_list_pdf_bytes(listing_date: datetime) -> bytes:
    listing_date_str = listing_date.strftime("%d/%m/%Y")
    payload = {
        "coram": "",
        "coramOrder": "",
        "sidecode": "",
        "listflag": "5",
        "courtcode": "0",
        "courtroom": "undefined-undefined-undefined",
        "listingdate": listing_date_str,
        "advocatecodeval": "",
        "advocatenameval": "",
        "ccinval": "",
        "download_token": "",
    }

    with httpx.Client(
        headers=CAUSE_LIST_HEADERS, follow_redirects=True, timeout=60.0, verify=False
    ) as client:
        home_response = client.get(CAUSE_LIST_HOME_URL)
        home_response.raise_for_status()
        token_match = re.search(
            r'name="download_token"\s+value="([^"]*)"', home_response.text
        )
        if token_match:
            payload["download_token"] = token_match.group(1)

        response = client.post(CAUSE_LIST_PRINT_URL, data=payload)
        response.raise_for_status()
        return response.content


class GujaratHCService:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0'
        }
        self.ocr = ddddocr.DdddOcr(show_ad=False)
        self.case_types_map = {}
        self.case_types_path = Path(__file__).with_name("gujarat_case_types.json")

    def _refresh_session(self):
        """Visit main page to establish session/cookies."""
        try:
            self.session.get(f"{BASE_URL}/", timeout=30)
        except Exception as e:
            logger.warning(f"Failed to refresh session: {e}")

    def solve_captcha(self) -> Optional[str]:
        """Download and solve CAPTCHA."""
        try:
            ts = int(time.time() * 1000)
            url = CAPTCHA_URL.format(ts)
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                res = self.ocr.classification(resp.content)
                return res
        except Exception as e:
            logger.error(f"Error solving CAPTCHA: {e}")
        return None

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def get_case_types(self) -> Dict[str, str]:
        """Fetch and cache case types (Name -> Code)."""
        if self.case_types_map:
            return self.case_types_map

        if self.case_types_path.exists():
            try:
                with self.case_types_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and data:
                    self.case_types_map = data
                    return self.case_types_map
                logger.warning("Case types file is empty or invalid; refetching.")
            except Exception as e:
                logger.warning(f"Failed to read case types file: {e}; refetching.")

        self._refresh_session()
        try:
            resp = self.session.post(CASE_TYPE_URL, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            # Parse case types
            # Structure: finaldata[0].casetypearray[].Civil[].{casetype, casecode}
            groups = data.get('finaldata', [])[0].get('casetypearray', [])
            mapping = {}
            for group in groups:
                for category in ['Civil', 'Criminal', 'OJ']:
                    if category in group:
                        for item in group[category]:
                            name = item.get('casetype')
                            code = item.get('casecode')
                            if name and code:
                                mapping[name] = code
            
            self.case_types_map = mapping
            try:
                with self.case_types_path.open("w", encoding="utf-8") as f:
                    json.dump(self.case_types_map, f, ensure_ascii=True, indent=2, sort_keys=True)
            except Exception as e:
                logger.warning(f"Failed to persist case types file: {e}")
            return mapping
        except Exception as e:
            logger.error(f"Failed to fetch case types: {e}")
            raise

    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str or date_str.strip() in ['-', '', 'NA']:
            return None
        try:
            return datetime.strptime(date_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def fetch_order_document(self, order_params: Dict[str, str]) -> requests.Response:
        """
        Fetch the actual PDF content for an order.
        order_params should contain ccin_no, order_no, order_date, flag, casedetail, nc.
        """
        url = f"{BASE_URL}/OrderHistoryViewDownload"
        data = {
            'ccin_no': order_params.get('ccin_no'),
            'order_no': order_params.get('order_no'),
            'order_date': order_params.get('order_date'),
            'flag': order_params.get('flag', 'v'),
            'casedetail': order_params.get('casedetail'),
            'nc': order_params.get('nc', '-'),
            'download_token_value_id': str(int(time.time() * 1000))
        }
        return self.session.post(url, data=data, timeout=30)

    def _parse_details(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse the JSON response into a normalized format."""
        
        result = {
            "status": None,
            "cin_no": None,
            "registration_no": None,
            "registration_date": None,
            "filing_no": None,
            "filing_date": None,
            "case_type": None,
            "case_no": None,
            "case_year": None,
            "pet_name": [],
            "res_name": [],
            "pet_advocates": [],
            "res_advocates": [],
            "judges": None,
            "court_name": "Gujarat High Court",
            "bench_name": None,
            "district": None,
            "first_hearing_date": None,
            "next_hearing_date": None,
            "decision_date": None,
            "orders": [],
            "history": [],
            "raw_data": data  # Keep raw data for debugging/completeness
        }

        # 4. Main Details
        if len(data) > 4 and 'maindetails' in data[4]:
            main = data[4]['maindetails'][0]
            result['cin_no'] = main.get('ccin')
            result['status'] = main.get('casestatus')
            result['registration_date'] = self._parse_date(main.get('registration_date'))
            result['filing_no'] = main.get('stampnumber')
            result['filing_date'] = self._parse_date(main.get('presentdate')) # "Presented On"
            result['case_type'] = main.get('casetype')
            result['case_no'] = main.get('casenumber')
            result['case_year'] = main.get('caseyear')
            result['bench_name'] = main.get('benchname')
            result['district'] = main.get('districtname')
            result['judges'] = main.get('judges')
            result['next_hearing_date'] = self._parse_date(main.get('listingdate')) # Often listingdate is next date
            result['decision_date'] = self._parse_date(main.get('disposaldate'))
            
            # Format registration number: TYPE/NO/YEAR
            if result['case_type'] and result['case_no'] and result['case_year']:
                result['registration_no'] = f"{result['case_type']}/{result['case_no']}/{result['case_year']}"

        # 0. Litigant (Petitioner)
        if len(data) > 0 and 'litigant' in data[0]:
            for item in data[0]['litigant']:
                name = item.get('litigantname')
                if name:
                    result['pet_name'].append(name)

        # 1. Respondent
        if len(data) > 1 and 'respondant' in data[1]:
            for item in data[1]['respondant']:
                name = item.get('respondantname')
                if name:
                    result['res_name'].append(name)

        # 2. Advocate
        if len(data) > 2 and 'advocate' in data[2]:
            for item in data[2]['advocate']:
                adv_name = item.get('advocatename')
                l_type = item.get('litiganttypecode') # 1=Pet, 2=Res
                if l_type == '1':
                    result['pet_advocates'].append(adv_name)
                elif l_type == '2':
                    result['res_advocates'].append(adv_name)

        # 10. Court Proceedings (History)
        if len(data) > 10 and 'linkedmatterscp' in data[10]:
            for item in data[10]['linkedmatterscp']:
                entry = {
                    "business_date": self._parse_date(item.get('PROCEEDINGDATElmcp')),
                    "hearing_date": self._parse_date(item.get('PROCEEDINGDATElmcp')), # Assuming same
                    "judge": item.get('JUDGESlmcp'),
                    "purpose": item.get('STAGENAMElmcp'),
                    "result": item.get('ACTIONNAMElmcp')
                }
                result['history'].append(entry)

        # 11. Order History
        if len(data) > 11 and 'orderhistory' in data[11]:
            for item in data[11]['orderhistory']:
                # Construct logical document_url with parameters
                params = {
                    "ccin_no": item.get('ccinoh'),
                    "order_no": item.get('ordernooh'),
                    "order_date": item.get('orderdate'),
                    "flag": 'v',
                    "casedetail": item.get('descriptionoh'),
                    "nc": item.get('nc', '-')
                }
                # Create a pseudo-URL that contains all necessary info
                param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                doc_url = f"{BASE_URL}/OrderHistoryViewDownload?{param_str}"

                order = {
                    "date": self._parse_date(item.get('orderdate')),
                    "description": f"{item.get('descriptionoh')} | {item.get('judgesoh')} | {item.get('orderdate')}",
                    "judge": item.get('judgesoh'),
                    "order_no": item.get('ordernooh'),
                    "ccin": item.get('ccinoh'),
                    "document_url": doc_url
                }
                result['orders'].append(order)

        return result

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_case_details(self, case_type_name: str, case_no: str, case_year: str) -> Optional[Dict[str, Any]]:
        """
        Fetch case details by Case Type (Name), Number, and Year.
        e.g. fetch_case_details("SCA", "7966", "2025")
        """
        self._refresh_session()
        
        # 1. Get Case Code
        types = self.get_case_types()
        case_code = types.get(case_type_name.upper()) or types.get(case_type_name)
        if not case_code:
            logger.error(f"Case type '{case_type_name}' not found.")
            # Fallback: maybe the user passed the code directly?
            if case_type_name.isdigit():
                 case_code = case_type_name
            else:
                 return None

        # 2. Solve Captcha
        captcha = self.solve_captcha()
        if not captcha:
            raise ValueError("Failed to solve CAPTCHA")

        # 3. Construct GENCCIN
        # Format: R#<case_code>#<case_no>#<case_year>
        # Ensure 3 digit case code (padded), 5 digit case no (padded) - handled by server usually but JS does padding
        # JS: while (casecode.length < 3) casecode = "0" + casecode;
        # JS: while (casenumber.length < 5) casenumber = "0" + casenumber;
        
        case_code = str(case_code).zfill(3)
        
        genccin = f"R#{case_code}#{case_no}#{case_year}"
        
        # 4. Fetch Data
        data = {
            'ccin': genccin,
            'servicecode': '1',
            'challengeString': captcha
        }
        
        resp = self.session.post(DATA_URL, data=data, timeout=30)
        resp.raise_for_status()
        
        json_resp = resp.json()
        
        # Check for errors in response
        if 'finaldata' in json_resp:
            error_msg = json_resp['finaldata'][0].get('ERROR')
            if error_msg:
                if "captcha" in error_msg.lower():
                    logger.warning(f"Server returned error: {error_msg}. Retrying...")
                    raise ValueError(f"Server error: {error_msg}")
                logger.error(f"Server returned error: {error_msg}")
                return None
             
        if 'data' in json_resp:
            parsed = self._parse_details(json_resp['data'])
            if parsed and not parsed.get("registration_no"):
                logger.warning("Missing registration_no for case %s/%s/%s; retrying.", case_type_name, case_no, case_year)
                raise ValueError("Missing registration_no")
            return parsed
        
        return None

# Global instance
_service = GujaratHCService()

def get_gujarat_case_details(case_type: str, case_no: str, case_year: str):
    return _service.fetch_case_details(case_type, case_no, case_year)

def _fetch_order_document(url: str, referer: str | None = None) -> requests.Response:
    """
    Helper to fetch order documents, handling both regular URLs and the pseudo-URLs
    generated for OrderHistoryViewDownload.
    """
    if "OrderHistoryViewDownload?" in url:
        parsed = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        return _service.fetch_order_document(params)
    
    headers = {}
    if referer:
        headers["Referer"] = referer
    return _service.session.get(url, timeout=30, headers=headers)

async def persist_orders_to_storage(
    orders: list[dict] | None,
    case_id: str | None = None,
) -> list[dict] | None:
    """
    Upload scraped Gujarat HC order documents to storage and update their URLs.
    """
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer=f"{BASE_URL}/",
    )


def _create_cron_job_run(supabase, job_name: str, metadata: Dict[str, Any]) -> str | None:
    try:
        payload = {"job_name": job_name, "status": "running", "metadata": metadata}
        result = supabase.table(CRON_JOB_RUNS_TABLE).insert(payload).execute()
        if result.data:
            return result.data[0].get("id")
    except Exception as exc:
        logger.warning("Failed to create cron job run: %s", exc)
    return None


def _finish_cron_job_run(
    supabase,
    run_id: str | None,
    status: str,
    summary: Dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    if not run_id:
        return
    payload: Dict[str, Any] = {
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


def _build_case_label(case_record: Dict[str, Any]) -> str:
    case_no = case_record.get("case_no")
    cin_no = case_record.get("cin_no")
    registration_no = case_record.get("registration_no")
    parts = [p for p in [case_no, cin_no, registration_no] if p]
    return " | ".join(parts) if parts else f"Case {case_record.get('id')}"


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 3)]}..."


def _get_or_create_case_notification(
    supabase,
    workspace_id: str,
    target_user_id: str,
    event_key: str,
    title: str,
    message: str,
    redirect_uri: str,
    metadata: Dict[str, Any],
) -> bool:
    existing = (
        supabase.table(VOTUM_NOTIFICATIONS_TABLE)
        .select("id")
        .eq("workspace_id", workspace_id)
        .eq("target_user_id", target_user_id)
        .eq("type", "case")
        .eq("subtype", "cause_list_entry")
        .contains("metadata", {"case_id": metadata.get("case_id"), "event_key": event_key})
        .limit(1)
        .execute()
    )
    if existing.data:
        return False

    insert_payload = {
        "type": "case",
        "subtype": "cause_list_entry",
        "module": "case",
        "redirect_uri": redirect_uri,
        "title": title,
        "message": message,
        "workspace_id": workspace_id,
        "target_user_id": target_user_id,
        "created_by_id": None,
        "related_entity_type": "case",
        "is_obsolete": False,
        "metadata": metadata | {"event_key": event_key},
    }
    supabase.table(VOTUM_NOTIFICATIONS_TABLE).insert(insert_payload).execute()
    return True


async def _notify_cause_list_entry(
    supabase,
    case_record: Dict[str, Any],
    entry: Dict[str, Any],
    listing_date: str,
) -> None:
    workspace_id = case_record.get("workspace_id")
    case_id = case_record.get("id")
    if not workspace_id or case_id is None:
        return

    case_label = _build_case_label(case_record)
    entry_text = (entry.get("text") or "").strip()
    subject = f"New cause list entry: {case_label}"
    body = f"{case_label} has a new cause list entry for {listing_date}."
    if entry_text:
        body = f"{body} {entry_text}"
    body = _truncate(body, 320)
    sms_body = _truncate(body, 150)
    redirect_uri = f"/home/cases/{case_id}"
    event_key = entry.get("entry_hash") or str(entry.get("id") or "")
    metadata = {
        "case_id": str(case_id),
        "cin_no": case_record.get("cin_no"),
        "listing_date": listing_date,
        "entry_hash": entry.get("entry_hash"),
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
            sms_result = send_sms_message(contact_value, sms_body)
            if not sms_result.get("success"):
                logger.warning(
                    "Failed to send reminder SMS to %s: %s",
                    contact_value,
                    sms_result.get("error"),
                )

    assigned_user_ids = case_record.get("assigned_user_ids") or []
    assigned_user_emails: Dict[str, str] = {}
    assigned_user_phones: Dict[str, str] = {}
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

        created = _get_or_create_case_notification(
            supabase,
            workspace_id,
            user_id,
            event_key,
            subject,
            body,
            redirect_uri,
            metadata,
        )
        if not created:
            continue
        try:
            await send_fcm_notification(user_id, subject, body, supabase)
        except Exception as exc:
            logger.warning("Failed to send push notification to %s: %s", user_id, exc)


@router.post("/cause_list/sync")
async def sync_cause_list(
    listing_date: Optional[str] = Form(None),
    dry_run: bool = Form(False),
    limit: Optional[int] = Form(None),
):
    """
    Fetch the next day's cause list, extract matching case rows by registration number,
    and store extracted text + image path in votum_cases.cause_list_entries.
    """
    supabase = get_supabase_client()
    run_id = _create_cron_job_run(
        supabase,
        "cause_list_sync",
        {"listing_date": listing_date, "dry_run": dry_run, "limit": limit},
    )
    run_status = "failed"
    run_error: str | None = None
    run_summary: Dict[str, Any] | None = None
    pdf_path: str | None = None

    try:
        try:
            target_date = parse_listing_date(listing_date)
        except ValueError as e:
            run_error = str(e)
            raise HTTPException(status_code=400, detail=str(e))

        listing_date_token = target_date.strftime("%Y%m%d")

        try:
            pdf_bytes = fetch_cause_list_pdf_bytes(target_date)
        except Exception as e:
            run_error = f"Failed to fetch cause list PDF: {str(e)}"
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch cause list PDF: {str(e)}",
            )

        with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(pdf_bytes)
            pdf_path = tmp_pdf.name

        storage_bucket = "documents"

        case_query = (
            supabase.table("votum_cases")
            .select(
                "id, workspace_id, registration_no, cause_list_entries, case_no, cin_no, "
                "assigned_user_ids, reminder_contacts"
            )
            .execute()
        )
        cases = case_query.data or []
        if limit:
            cases = cases[:limit]

        totals = {
            "total_cases": len(cases),
            "matched_cases": 0,
            "entries_added": 0,
            "images_uploaded": 0,
            "entries_updated": 0,
        }
        case_results = []

        with TemporaryDirectory() as tmp_dir:
            for case in cases:
                registration_no = case.get("registration_no")
                if not registration_no:
                    continue

                entries = find_case_entries(pdf_path, registration_no)
                if not entries:
                    continue

                case_id = case["id"]
                workspace_id = case.get("workspace_id")
                if not workspace_id:
                    continue
                existing_entries: List[Dict[str, Any]] = (
                    case.get("cause_list_entries") or []
                )
                existing_hash_index = {
                    entry.get("entry_hash"): idx
                    for idx, entry in enumerate(existing_entries)
                    if entry.get("entry_hash")
                }
                existing_image_paths = {
                    entry.get("image_path")
                    for entry in existing_entries
                    if entry.get("image_path")
                }

                case_out_dir = os.path.join(tmp_dir, f"case_{case_id}")
                image_paths = save_screenshots(
                    pdf_path, entries, case_out_dir, padding=10, dpi=200
                )

                new_entries: List[Dict[str, Any]] = []
                entries_updated = 0
                images_uploaded = 0

                for idx, entry in enumerate(entries, start=1):
                    entry_text = (entry.get("text") or "").strip()
                    entry_hash = hashlib.sha256(
                        f"{listing_date_token}:{entry.get('page')}:{entry.get('bbox')}:{entry_text}".encode(
                            "utf-8"
                        )
                    ).hexdigest()[:12]

                    image_file_name = (
                        f"cause_list_{listing_date_token}_p{entry.get('page')}_{entry_hash}.png"
                    )
                    image_file_path = (
                        f"{workspace_id}/case-{case_id}/cause-list/{image_file_name}"
                    )
                    image_uploaded = False

                    image_index = idx - 1
                    if (
                        image_index < len(image_paths)
                        and os.path.exists(image_paths[image_index])
                        and image_file_path not in existing_image_paths
                    ):
                        with open(image_paths[image_index], "rb") as img_file:
                            img_bytes = img_file.read()
                        if not dry_run:
                            supabase.storage.from_(storage_bucket).upload(
                                path=image_file_path,
                                file=img_bytes,
                                file_options={"content-type": "image/png", "upsert": True},
                            )
                            public_url = supabase.storage.from_(
                                storage_bucket
                            ).get_public_url(image_file_path)
                            supabase.table("documents").insert(
                                {
                                    "workspace_id": workspace_id,
                                    "user_id": None,
                                    "pdf_url": public_url,
                                    "filename": image_file_name,
                                    "tags": [],
                                    "annotations": [],
                                    "folder_id": None,
                                    "document_type": "cause_list",
                                    "status": "uploaded",
                                    "metadata": {
                                        "source": "cause_list",
                                        "uploaded_by_name": "System",
                                        "file_size": len(img_bytes),
                                        "file_type": "image/png",
                                        "storage_path": image_file_path,
                                        "storage_bucket": storage_bucket,
                                    },
                                    "case_id": case_id,
                                }
                            ).execute()
                        image_uploaded = True
                        images_uploaded += 1

                    entry_payload: Dict[str, Any] = {
                        "id": str(uuid.uuid4()),
                        "entry_hash": entry_hash,
                        "listing_date": target_date.strftime("%Y-%m-%d"),
                        "page": entry.get("page"),
                        "bbox": entry.get("bbox"),
                        "text": entry_text,
                        "image_path": image_file_path
                        if image_uploaded or image_file_path in existing_image_paths
                        else None,
                        "created_at": datetime.now().isoformat(),
                    }

                    if entry_hash in existing_hash_index:
                        existing_entry = existing_entries[
                            existing_hash_index[entry_hash]
                        ]
                        updated = False
                        if entry_text and not existing_entry.get("text"):
                            existing_entry["text"] = entry_text
                            updated = True
                        if (
                            entry_payload["image_path"]
                            and not existing_entry.get("image_path")
                        ):
                            existing_entry["image_path"] = entry_payload["image_path"]
                            updated = True
                        if updated:
                            entries_updated += 1
                        continue

                    new_entries.append(entry_payload)

                if (new_entries or entries_updated) and not dry_run:
                    updated_entries = existing_entries + new_entries
                    supabase.table("votum_cases").update(
                        {"cause_list_entries": updated_entries}
                    ).eq("id", case_id).execute()

                    for entry_payload in new_entries:
                        await _notify_cause_list_entry(
                            supabase,
                            case,
                            entry_payload,
                            target_date.strftime("%Y-%m-%d"),
                        )

                totals["matched_cases"] += 1
                totals["entries_added"] += len(new_entries)
                totals["entries_updated"] += entries_updated
                totals["images_uploaded"] += images_uploaded
                case_results.append(
                    {
                        "case_id": case_id,
                        "registration_no": registration_no,
                        "entries_found": len(entries),
                        "entries_added": len(new_entries),
                        "entries_updated": entries_updated,
                    }
                )

        run_status = "success"
        run_summary = {
            "listing_date": target_date.strftime("%Y-%m-%d"),
            "dry_run": dry_run,
            "totals": totals,
        }
        return {
            "status": "success",
            "listing_date": target_date.strftime("%Y-%m-%d"),
            "dry_run": dry_run,
            "totals": totals,
            "cases": case_results,
        }
    except Exception as exc:
        if run_error is None:
            run_error = str(exc)
        raise
    finally:
        _finish_cron_job_run(
            supabase,
            run_id,
            run_status,
            summary=run_summary,
            error=run_error,
        )
        if pdf_path and os.path.exists(pdf_path):
            os.remove(pdf_path)

if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(get_gujarat_case_details("21", "7966", "2025")))
