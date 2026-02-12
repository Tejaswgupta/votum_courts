import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urljoin

import ddddocr
import requests
from bs4 import BeautifulSoup
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from .order_storage import \
    persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://efiling.nclat.gov.in"
MAIN_URL = f"{BASE_URL}/mainPage.drt"
CASE_STATUS_URL = f"{BASE_URL}/nclat/case_status.php"
AJAX_URL = f"{BASE_URL}/nclat/ajax/ajax.php"
CAPTCHA_URL = f"{BASE_URL}/nclat/captcha.php"

# /nclat/order_view.php?path=... returns a PDF
ORDERS_VIEW_PREFIX = f"{BASE_URL}/nclat/order_view.php"

DEFAULT_UA = os.getenv(
    "NCLAT_SCRAPER_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
)

# Captcha is simple alpha-numeric in most cases.
CAPTCHA_TOKEN_RE = re.compile(r"[A-Z0-9]+", re.IGNORECASE)

CASE_TYPE_NAME_TO_ID: dict[str, str] = {
    "company appeal(at)": "32",
    "company appeal(at)(ins)": "33",
    "competition appeal(at)": "34",
    "interlocutory application": "35",
    "compensation application": "36",
    "contempt case(at)": "37",
    "review application": "38",
    "restoration application": "39",
    "transfer appeal": "40",
    "transfer original petition (mrtp-at)": "61",
}


def _normalize_location(location: str | None) -> str:
    """
    The portal expects schema_name/location as 'delhi' or 'chennai'.
    """
    value = (location or "").strip().lower()
    if not value:
        return "delhi"
    if "chennai" in value:
        return "chennai"
    return "delhi"


def _normalize_case_type(case_type: str | None) -> str | None:
    value = (case_type or "").strip()
    if not value:
        return None
    if value.isdigit():
        return value
    key = re.sub(r"\s+", " ", value.lower())
    return CASE_TYPE_NAME_TO_ID.get(key)


def _normalize_date(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Fallback: find dd-mm-yyyy-ish
    m = re.search(r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})", raw)
    if not m:
        return None
    d, mo, y = m.groups()
    y = f"20{y}" if len(y) == 2 else y
    try:
        return datetime(int(y), int(mo), int(d)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _split_title(case_title: str | None) -> tuple[str | None, str | None]:
    text = re.sub(r"\s+", " ", (case_title or "")).strip()
    if not text:
        return None, None
    parts = re.split(r"\bVS\b|\bV/S\b|\bV\.S\.?\b", text, flags=re.IGNORECASE)
    if len(parts) >= 2:
        left = parts[0].strip() or None
        right = " ".join(p.strip() for p in parts[1:]).strip() or None
        return left, right
    return text, None


def _new_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_UA,
            "Origin": BASE_URL,
            "Accept": "text/html, */*; q=0.01",
        }
    )
    return session


def _bootstrap_case_status(session: requests.Session) -> None:
    """
    The case status page blocks "direct access"; bootstrap by:
    1) GET main page to obtain srfCaseStatus token.
    2) POST it to /nclat/case_status.php to establish PHPSESSID and allow access.
    """
    resp = session.get(MAIN_URL, timeout=30, headers={"Referer": MAIN_URL})
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    token_el = soup.select_one("form#form_casestatus input[name=srfCaseStatus]")
    token = token_el.get("value") if token_el else None
    if not token:
        raise RuntimeError("NCLAT bootstrap failed: missing srfCaseStatus token.")

    post = session.post(
        CASE_STATUS_URL,
        data={"srfCaseStatus": token},
        timeout=30,
        headers={"Referer": MAIN_URL},
    )
    post.raise_for_status()
    if "Direct access not allowed" in post.text:
        raise RuntimeError("NCLAT bootstrap failed: case_status still blocked.")


def _ensure_ready(session: requests.Session) -> None:
    # We consider having PHPSESSID a good proxy that the bootstrap completed.
    if session.cookies.get("PHPSESSID"):
        return
    _bootstrap_case_status(session)


def _solve_captcha(session: requests.Session) -> str:
    ocr = ddddocr.DdddOcr(show_ad=False)
    # Try a few times; captcha refreshes on each request.
    for attempt in range(8):
        url = f"{CAPTCHA_URL}?_={int(time.time() * 1000)}_{attempt}"
        resp = session.get(url, timeout=30, headers={"Referer": CASE_STATUS_URL})
        resp.raise_for_status()
        raw = (ocr.classification(resp.content) or "").strip()
        token = "".join(CAPTCHA_TOKEN_RE.findall(raw)).strip()
        if token:
            return token
    raise RuntimeError("NCLAT captcha OCR failed after multiple attempts.")


def _ajax_post(session: requests.Session, data: dict[str, Any]) -> str:
    _ensure_ready(session)
    headers = {
        "Referer": CASE_STATUS_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = session.post(AJAX_URL, data=data, timeout=30, headers=headers)
    resp.raise_for_status()
    return resp.text or ""


def _parse_search_results(html: str, location: str) -> list[dict]:
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.find("table")
    if not table:
        return []

    results: list[dict] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        filing_no = tds[1].get_text(" ", strip=True)
        case_no = tds[2].get_text(" ", strip=True)
        title = tds[3].get_text(" ", strip=True)
        reg_date_raw = tds[4].get_text(" ", strip=True)

        if not filing_no or not re.fullmatch(r"\d{10,}", filing_no):
            continue

        pet, res = _split_title(title)
        results.append(
            {
                "cino": filing_no,
                "filing_no": filing_no,
                "case_no": case_no or None,
                "case_title": title or None,
                "pet_name": pet,
                "res_name": res,
                "date_of_decision": None,
                "registration_date": _normalize_date(reg_date_raw) or reg_date_raw or None,
                "type_name": None,
                "bench": location,
                "court_name": "NCLAT",
            }
        )
    return results


def _parse_details(html: str, location: str, filing_no: str) -> dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    tables = soup.find_all("table")

    title_text = None
    if tables:
        title_text = tables[0].get_text(" ", strip=True) or None
    pet_title, res_title = _split_title(title_text)

    filing_date: str | None = None
    registration_date: str | None = None
    case_no: str | None = None
    status: str | None = None

    def _norm_key(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (text or "").strip().lower()).strip()

    def _is_case_detail_kv_table(table) -> bool:
        # Avoid connected-cases grids which have headers like "Sr. No. | Filing No | Case No | Date of filing..."
        header_text = " ".join(th.get_text(" ", strip=True).lower() for th in table.find_all("th"))
        if "sr. no" in header_text or "sr no" in header_text:
            return False

        rows = table.find_all("tr")
        for tr in rows[:3]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            # Expected shape includes: Filing No, <digits>, (spacer), Date Of Filing, <date>
            if len(cells) >= 5:
                k1, v1, _, k2, v2 = cells[:5]
                if (
                    _norm_key(k1) == "filing no"
                    and re.fullmatch(r"\d{10,}", (v1 or "").strip())
                    and _norm_key(k2) == "date of filing"
                    and (v2 or "").strip()
                ):
                    return True
        return False

    # Table with Filing No/Date of filing/Case No/Registration Date/Status.
    # The markup can include spacer cells, so we read label/value pairs with a sliding window.
    for t in tables:
        if not _is_case_detail_kv_table(t):
            continue

        for tr in t.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if not cells:
                continue

            if len(cells) == 2 and _norm_key(cells[0]) == "status":
                status = cells[1].strip() or None
                continue

            i = 0
            while i + 1 < len(cells):
                k = cells[i].strip()
                v = cells[i + 1].strip()
                if not k or not v:
                    i += 1
                    continue
                kn = _norm_key(k)
                if kn in {"filing no", "filing number"}:
                    filing_no = v or filing_no
                elif kn == "date of filing":
                    filing_date = _normalize_date(v) or v
                elif kn in {"case no", "case number"}:
                    case_no = v or case_no
                elif kn == "registration date":
                    registration_date = _normalize_date(v) or v
                i += 2
        break

    petitioners: list[str] = []
    respondents: list[str] = []
    pet_advs: list[str] = []
    res_advs: list[str] = []

    def _collect_from_two_col_table(table, header_substr: str) -> list[str]:
        out: list[str] = []
        if not table:
            return out
        headers = [c.get_text(" ", strip=True).lower() for c in table.find_all("th")]
        if not any(header_substr in h for h in headers):
            return out
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                value = tds[1].get_text(" ", strip=True)
                if value and value.lower() != "no data":
                    out.append(value)
        return out

    # Party tables may appear as separate 2-col tables.
    for t in tables:
        petitioners.extend(_collect_from_two_col_table(t, "applicant/appellant"))
        respondents.extend(_collect_from_two_col_table(t, "respodent"))

    # Legal rep tables similarly.
    for t in tables:
        pet_advs.extend(_collect_from_two_col_table(t, "legal representative"))
        # Respondent legal rep table has "Respodent Legal Representative Name"
        headers = [c.get_text(" ", strip=True).lower() for c in t.find_all("th")]
        if any("respodent" in h and "legal representative" in h for h in headers):
            for tr in t.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    value = tds[1].get_text(" ", strip=True)
                    if value and value.lower() != "no data":
                        res_advs.append(value)

    # Order history: rows with Download and order_view.php links.
    orders: list[dict] = []
    for t in tables:
        ths = [th.get_text(" ", strip=True).lower() for th in t.find_all("th")]
        if not ths:
            continue
        if "order date" in " ".join(ths) and "order type" in " ".join(ths) and "view" in " ".join(ths):
            for tr in t.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue
                order_date_raw = tds[1].get_text(" ", strip=True)
                order_type = tds[2].get_text(" ", strip=True)
                href = None
                link = tr.find("a", href=True)
                if link:
                    href = link["href"]
                else:
                    # Sometimes Download is rendered without an <a>; fall back to any href in doc.
                    any_link = soup.find("a", href=re.compile(r"order_view\.php\?path="))
                    if any_link:
                        href = any_link.get("href")

                document_url = urljoin(f"{BASE_URL}/nclat/", href) if href else None
                orders.append(
                    {
                        "date": _normalize_date(order_date_raw) or order_date_raw or None,
                        "description": order_type or "Order",
                        "document_url": document_url,
                        "source_document_url": document_url,
                        "order_type": order_type or None,
                    }
                )
            break

    # Hearing table: keep as raw list for now; can be enriched via case_details_hearing calls.
    hearings: list[dict] = []
    for t in tables:
        ths = [th.get_text(" ", strip=True).lower() for th in t.find_all("th")]
        if not ths:
            continue
        if "hearing date" in " ".join(ths) and "purpose" in " ".join(ths):
            for tr in t.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue
                hearings.append(
                    {
                        "hearing_date": _normalize_date(tds[1].get_text(" ", strip=True))
                        or tds[1].get_text(" ", strip=True)
                        or None,
                        "court_no": tds[2].get_text(" ", strip=True) or None,
                        "purpose": tds[3].get_text(" ", strip=True) or None,
                    }
                )
            break

    return {
        "cin_no": filing_no,
        "filling_no": filing_no,
        "case_no": case_no,
        "filing_date": filing_date,
        "registration_date": registration_date,
        "bench_name": location,
        "court_name": "NCLAT",
        "pet_name": petitioners or ([pet_title] if pet_title else []),
        "res_name": respondents or ([res_title] if res_title else []),
        "petitioner_advocates": sorted({a for a in pet_advs if a}),
        "respondent_advocates": sorted({a for a in res_advs if a}),
        "next_listing_date": None,
        "orders": orders,
        "history": [],
        "additional_info": {
            "status": status,
            "case_title": title_text,
            "hearings": hearings,
            "location": location,
        },
        "original_html": html,
    }


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def nclat_search_by_case_no(
    location: str,
    case_type: str,
    case_no: str,
    case_year: str,
) -> list[dict]:
    """
    Basic details: search by case number.
    Returns rows including filing_no (used for complete details).
    """
    schema = _normalize_location(location)
    ctype = _normalize_case_type(case_type)
    if not ctype:
        raise ValueError("case_type is required (id like '33' or known name).")
    if not (case_no or "").strip():
        raise ValueError("case_no is required.")

    session = _new_session()
    for attempt in range(8):
        captcha = _solve_captcha(session)
        html = _ajax_post(
            session,
            {
                "action": "case_status_search",
                "search_by": "3",
                "case_type": ctype,
                "case_number": str(case_no).strip(),
                "case_year": (case_year or "").strip() or "All",
                "answer": captcha,
                "schema_name": schema,
            },
        )
        if "Captch Value is incorrect" in html:
            continue
        return _parse_search_results(html, location=schema)

    return []


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def nclat_search_by_free_text(
    location: str,
    search_by: str,
    free_text: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict]:
    """
    Basic details: free text search.
    The frontend currently passes (search_by, free_text, from_date, to_date).
    We support:
    - search_by in {'party','4'} => By Party
    - search_by in {'advocate','5'} => By Advocate
    - search_by in {'filing','1'} => Filing No (uses free_text as diary_no/filing-like)
    - search_by in {'case_type','2'} => Case Type (uses free_text as case_type id/name; returns possibly many)
    """
    schema = _normalize_location(location)
    sb_raw = (search_by or "").strip().lower()
    if sb_raw in {"4", "party", "by party"}:
        sb = "4"
    elif sb_raw in {"5", "advocate", "by advocate"}:
        sb = "5"
    elif sb_raw in {"1", "filing", "filing no", "filing_no"}:
        sb = "1"
    elif sb_raw in {"2", "case type", "case_type"}:
        sb = "2"
    else:
        raise ValueError("search_by must be one of: 1,2,4,5 (filing/case_type/party/advocate).")

    session = _new_session()
    for attempt in range(8):
        captcha = _solve_captcha(session)
        payload: dict[str, Any] = {
            "action": "case_status_search",
            "search_by": sb,
            "case_year": "All",
            "answer": captcha,
            "schema_name": schema,
        }

        text = (free_text or "").strip()
        if sb == "4":
            payload["select_party"] = "1"
            payload["party_name"] = text
        elif sb == "5":
            payload["advocate_name"] = text
        elif sb == "1":
            payload["diary_no"] = text
        elif sb == "2":
            ctype = _normalize_case_type(text)
            if not ctype:
                raise ValueError("For search_by=2, free_text must be a case_type id/name.")
            payload["case_type"] = ctype
            payload["select_status"] = "all"

        if from_date:
            payload["from_date"] = from_date
        if to_date:
            payload["to_date"] = to_date

        html = _ajax_post(session, payload)
        if "Captch Value is incorrect" in html:
            continue
        return _parse_search_results(html, location=schema)

    return []


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def nclat_get_details(filing_no: str, bench: str | None = None) -> dict[str, Any] | None:
    """
    Complete details: fetch all details for a filing number.
    """
    if not (filing_no or "").strip():
        return None
    schema = _normalize_location(bench)

    session = _new_session()
    html = _ajax_post(
        session,
        {
            "action": "case_status_case_details",
            "filing_no": filing_no.strip(),
            "schema_name": schema,
        },
    )
    if "Direct access not allowed" in html:
        return None
    return _parse_details(html, location=schema, filing_no=filing_no.strip())


def _fetch_order_document(order_url: str, referer: str | None):
    session = _new_session()
    _ensure_ready(session)
    headers: dict[str, str] = {}
    if referer:
        headers["Referer"] = referer
    return session.get(order_url, timeout=30, headers=headers)


async def persist_orders_to_storage(
    orders: list[dict] | None,
    case_id: str | None = None,
) -> list[dict] | None:
    """
    Saving orders: download order PDFs (order_view.php) and upload to storage,
    updating each order's `document_url` to a stored URL.
    """
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        base_url=BASE_URL,
        referer=CASE_STATUS_URL,
    )
