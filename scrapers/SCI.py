import ast
import asyncio
import logging
import operator
import random
import re
import string
import os
import tempfile
from typing import Any, Dict, List, Optional

import ddddocr
import fitz
import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

try:
    from ..order_storage import persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    import sys
    from pathlib import Path
    # Add parent directory to path to allow importing order_storage
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from order_storage import persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TOKEN_URL = "https://www.sci.gov.in/case-status-party-name/"
CAPTCHA_URL = "https://www.sci.gov.in/?_siwp_captcha&id="
DATA_URL = "https://www.sci.gov.in/wp-admin/admin-ajax.php"

ocr = ddddocr.DdddOcr(show_ad=False)
session = requests.Session()
session.verify = False
BASE_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://www.sci.gov.in/case-status-party-name/',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=1',
    'TE': 'trailers'
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/123.0.0.0 Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0'
]

session.headers.update(BASE_HEADERS)


def _randomize_user_agent() -> None:
    session.headers['User-Agent'] = random.choice(USER_AGENTS)


def _session_get(url: str, **kwargs):
    _randomize_user_agent()
    return session.get(url, **kwargs)


_randomize_user_agent()


def validate_response(response):
    """Validate the response for successful status and expected structure."""
    if response.status_code != 200:
        logger.error(f"Unexpected status code: {response.status_code}")
        response.raise_for_status()
    
    try:
        return response.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON: {e}")
        raise


def _extract_html_fragment(payload: dict[str, Any]) -> Optional[str]:
    """Return HTML fragment from SCI payload or None when not available."""
    html_fragment = payload.get("data") if isinstance(payload, dict) else None

    if not html_fragment:
        logger.warning("SCI response did not include an HTML fragment: %s", payload)
        return None

    if isinstance(html_fragment, dict):
        message = html_fragment.get("message") if isinstance(html_fragment, dict) else html_fragment
        logger.warning(
            "SCI response returned a message instead of HTML: %s",
            message,
        )
        return None

    if isinstance(html_fragment, (list, tuple)):
        html_fragment = "".join(str(item) for item in html_fragment)

    if isinstance(html_fragment, bytes):
        html_fragment = html_fragment.decode("utf-8", errors="ignore")

    if not isinstance(html_fragment, str):
        logger.warning(
            "SCI response included unsupported data type %s for HTML fragment",
            type(html_fragment),
        )
        return None

    return html_fragment


def _normalize_captcha_expression(question: str) -> str:
    """Normalize OCR output into an arithmetic expression we can safely evaluate."""
    expr = question.strip()
    expr = expr.replace(" ", "")
    expr = expr.replace("=", "")
    expr = expr.replace("?", "")
    expr = expr.replace("×", "*")
    expr = expr.replace("x", "*")
    expr = expr.replace("X", "*")
    expr = expr.replace("÷", "/")
    expr = expr.replace(":", "/")
    expr = expr.replace("–", "-")
    expr = expr.replace("−", "-")

    allowed_chars = set("0123456789+-*/()")
    if not expr or any(ch not in allowed_chars for ch in expr):
        raise ValueError(f"Unsupported captcha expression: {question}")

    return expr


def _evaluate_captcha(question: str) -> int:
    """Safely evaluate a simple arithmetic captcha expression."""
    expr = _normalize_captcha_expression(question)

    allowed_operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
    }

    def _eval(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type not in allowed_operators:
                raise ValueError(f"Unsupported operator in captcha: {op_type}")
            return allowed_operators[op_type](_eval(node.left), _eval(node.right))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = _eval(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.Num):  # pragma: no cover  # for <3.8 compatibility
            return node.n
        raise ValueError(f"Unsupported captcha node: {ast.dump(node)}")

    result = _eval(ast.parse(expr, mode="eval"))

    if isinstance(result, int):
        return result
    if isinstance(result, float) and result.is_integer():
        return int(result)

    raise ValueError(f"Captcha did not resolve to an integer value: {result}")


def generate_random_string(length):
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))


def init_captcha(max_attempts: int = 3):
    """Fetch and solve the SCI captcha, retrying when OCR output is unusable."""

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        token_page = _session_get(TOKEN_URL)
        token_page_text = token_page.text
        # print('='*20)
        # print(token_page_text)
        # print('='*20)
        captcha_id = token_page_text.split('name="scid" value="')[1].split('"')[0]
        captcha_image = _session_get(CAPTCHA_URL + captcha_id).content
        question = ocr.classification(captcha_image)

        try:
            answer = _evaluate_captcha(question)
        except ValueError as exc:  # pragma: no cover - depends on OCR output
            last_error = exc
            logger.warning(
                "Unable to parse captcha question '%s' on attempt %d/%d", question, attempt, max_attempts
            )
            continue

        token_name = token_page_text.split('<input type="hidden" id="tok_')[1].split('"')[0]
        token_value = (
            token_page_text.split('<input type="hidden" id="tok_')[1]
            .split('value="')[1]
            .split('"')[0]
        )
        return token_name, token_value, captcha_id, question, answer

    logger.error("Failed to solve SCI captcha after %d attempts", max_attempts)
    if last_error is not None:
        raise requests.exceptions.RequestException("Unable to solve SCI captcha") from last_error
    raise requests.exceptions.RequestException("Unable to solve SCI captcha")




def table_to_list(soup):
    columns = []
    for col in soup.find_all("th"):
        columns.append(col.text.strip())

    rows = soup.find_all("tr")[1:]

    results = []
    for row in rows:
        cells = row.find_all("td")

        if len(cells) < len(columns):
            continue

        row_data = [cell.text.strip() for cell in cells]

        data = dict(zip(columns, row_data))

        results.append(
            {
                "cino": data.get("Diary Number", ""),
                "date_of_decision": None,
                "pet_name": data.get("Petitioner Name", ""),
                "res_name": data.get("Respondent Name", ""),
                "type_name": data.get("Status", ""),
            }
        )

    return results


def _extract_td_text(
    soup: BeautifulSoup, label: str, separator: str = " "
) -> Optional[str]:
    """Return stripped text of the <td> following a label when present."""
    label_node = soup.find(string=lambda text: text and label in text)
    if not label_node:
        return None
    cell = label_node.find_next("td") if hasattr(label_node, "find_next") else None
    if not cell:
        return None
    return cell.get_text(separator=separator, strip=True)


def _fetch_case_tab(
    diary_no: str, diary_year: str, tab_name: str
) -> tuple[Optional[str], dict[str, Any]]:
    """Fetch a specific SCI case tab and return the raw HTML fragment."""
    data = {
        "diary_no": diary_no,
        "diary_year": diary_year,
        "tab_name": tab_name,
        "action": "get_case_details",
        "es_ajax_request": "1",
    }
    resp = _session_get(DATA_URL, params=data)
    payload = validate_response(resp)

    if not payload.get("success"):
        logger.warning("SCI tab %s responded with success=False", tab_name)
        return None, payload

    html_fragment = _extract_html_fragment(payload)
    return html_fragment, payload


def _normalize_key(label: str) -> str:
    """Convert column labels into snake_case keys."""
    normalized = re.sub(r"[^0-9a-z]+", "_", label.lower())
    normalized = re.sub(r"_{2,}", "_", normalized)
    return normalized.strip("_")


def _parse_listing_dates(html_fragment: str) -> list[dict[str, Any]]:
    """Parse listing dates table into a list of row dictionaries."""
    soup = BeautifulSoup(html_fragment, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    header_row = None
    if table.thead:
        header_row = table.thead.find("tr")
    elif table.find("tr"):
        header_row = table.find("tr")

    headers: list[str] = []
    if header_row:
        headers = [cell.get_text(" ", strip=True) for cell in header_row.find_all(["th", "td"])]

    body_rows = table.tbody.find_all("tr") if table.tbody else table.find_all("tr")
    if header_row and body_rows and body_rows[0] is header_row:
        body_rows = body_rows[1:]

    results: list[dict[str, Any]] = []
    for row in body_rows:
        cells = row.find_all("td")
        if not cells:
            continue

        row_data: dict[str, Any] = {}
        for idx, cell in enumerate(cells):
            header = headers[idx] if idx < len(headers) else f"column_{idx + 1}"
            key = _normalize_key(header) or f"column_{idx + 1}"
            text_value = cell.get_text(" ", strip=True)
            row_data[key] = text_value

            link = cell.find("a")
            if link and link.get("href"):
                row_data[f"{key}_url"] = link.get("href")

        results.append(row_data)

    return results


def _parse_judgement_orders(html_fragment: str) -> list[dict[str, Any]]:
    """Extract judgment/order documents with date and description."""
    soup = BeautifulSoup(html_fragment, "html.parser")
    rows: list[dict[str, Any]] = []

    for cell in soup.find_all("td"):
        link = cell.find("a")
        if not link or not link.get("href"):
            continue

        date_text = link.get_text(" ", strip=True)
        full_text = cell.get_text(" ", strip=True)
        trailing = full_text[len(date_text) :].strip()
        trailing = trailing.replace("\xa0", " ").strip()
        if trailing.startswith("[") and trailing.endswith("]"):
            trailing = trailing[1:-1].strip()

        rows.append(
            {
                "date": date_text,
                "description": trailing or None,
                "document_url": link.get("href"),
            }
        )

    return rows


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_diary_number(diary_number, diary_year):
    token_name, token_value, captcha_id, question, answer = init_captcha()

    data = {
        "action": "get_case_status_diary_no",
        "diary_no": diary_number,
        "year": diary_year,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }

    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)

        if response.get("success"):
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")
            return table_to_list(soup)
        else:
            print("Request was unsuccessful.")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_case_number(case_type, case_number, case_year):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_case_status_case_no",
        "case_type": case_type,
        "case_no": case_number,
        "year": case_year,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        # print(resp.text)
        response = validate_response(resp)
        if not response["success"]:
            if "captcha" in response.get("data", "").lower():
                #call the function again to retry
                return sci_search_by_case_number(case_type, case_number, case_year)
            return []

        html_fragment = response.get("data", {}).get('resultsHtml',"")
        if not html_fragment:
            return []
        soup = BeautifulSoup(html_fragment, "html.parser")
        

        return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_aor_code(party_type, aor_code, year, case_status):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_case_status_aor_code",
        "party_type": party_type,
        "aor_code": aor_code,
        "year": year,
        "case_status": case_status,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        if response["success"]:
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")
            return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_party_name(party_type, party_name, year, party_status):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "party_type": party_type,
        "party_name": party_name,
        "year": year,
        "party_status": party_status,
        "scid": captcha_id,
        "tok_" + token_name: token_value,
        "siwp_captcha_value": answer,
        "es_ajax_request": "1",
        "submit": "Search",
        "action": "get_case_status_party_name",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        if response["success"]:
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")
            return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_court(
    court, state, bench, case_type, case_number, case_year, order_date
):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_case_status_court",
        "case_status_court": court,
        "case_status_state": state,
        "case_status_bench": bench,
        "case_status_case_type": case_type,
        "case_no": case_number,
        "year": case_year,
        "listing_date": order_date,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        if response["success"]:
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")

            return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_get_details(diary_no, diary_year):
    try:
        case_html, case_payload = _fetch_case_tab(diary_no, diary_year, "case_details")

        if not case_html:
            return None

        soup = BeautifulSoup(case_html, "html.parser")
        registration_no = (
            soup.find("h3").text.split("-")[1].strip() if soup.find("h3") else None
        )
        case_title = soup.find("h4").text.strip() if soup.find("h4") else None

        diary_info = _extract_td_text(soup, "Diary Number")
        diary_number = diary_info.split("Filed on", 1)[0].strip() if diary_info else None
        filing_date = None
        if diary_info:
            filing_parts = diary_info.split("Filed on", 1)
            if len(filing_parts) > 1:
                filing_date = filing_parts[1].split("[", 1)[0].strip()

        case_info = _extract_td_text(soup, "Case Number")
        case_number = (
            case_info.split("Registered", 1)[0].strip() if case_info else None
        )
        registration_date = None
        if case_info and "Registered on" in case_info:
            # Prefer the date immediately following "Registered on"
            m = re.search(r"Registered on\s*(\d{2}-\d{2}-\d{4})", case_info or "")
            if m:
                registration_date = m.group(1)

        verified_on = None
        if case_info and "Verified On" in case_info:
            verified_parts = case_info.split("Verified On", 1)
            verified_on = verified_parts[1].split("[", 1)[0].strip(" :")

        listed_info = _extract_td_text(soup, "Present/Last Listed On")
        # print('listed_info:', listed_info)
        listed_on = listed_info.split("[", 1)[0].strip() if listed_info else None
        status_info = _extract_td_text(soup, "Status/Stage")

        status = (
            status_info.split("List On", 1)[0].strip() if status_info else None
        )
        pending_or_disposed = 'pending' if 'pending' in status.lower() else 'disposed'
        category = _extract_td_text(soup, "Category")
        acts = [category]

        petitioners_raw = _extract_td_text(soup, "Petitioner(s)", separator="\n")
        petitioners = (
            [p.strip() for p in petitioners_raw.splitlines() if p.strip()]
            if petitioners_raw
            else None
        )
        respondents_raw = _extract_td_text(soup, "Respondent(s)", separator="\n")
        respondents = (
            [r.strip() for r in respondents_raw.splitlines() if r.strip()]
            if respondents_raw
            else None
        )
        petitioner_advocates_raw = _extract_td_text(
            soup, "Petitioner Advocate(s)", separator="\n"
        )
        petitioner_advocates = (
            [adv.strip() for adv in petitioner_advocates_raw.splitlines() if adv.strip()]
            if petitioner_advocates_raw
            else None
        )
        respondent_advocates_raw = _extract_td_text(
            soup, "Respondent Advocate(s)", separator="\n"
        )
        respondent_advocates = (
            [adv.strip() for adv in respondent_advocates_raw.splitlines() if adv.strip()]
            if respondent_advocates_raw
            else None
        )

        cin_no = _extract_td_text(soup, "CNR Number")

        judges = None
        if listed_info and "[" in listed_info and "]" in listed_info:
            judges_text = listed_info.split("[", 1)[1].split("]", 1)[0]
            judges = [j.strip() for j in judges_text.split("and") if j.strip()]

        listing_html = None
        listing_payload: Optional[dict[str, Any]] = None
        try:
            listing_html, listing_payload = _fetch_case_tab(
                diary_no, diary_year, "listing_dates"
            )
        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.warning("Failed to fetch SCI listing dates: %s", exc)

        listings = _parse_listing_dates(listing_html) if listing_html else []

        judgement_html = None
        judgement_payload: Optional[dict[str, Any]] = None
        try:
            judgement_html, judgement_payload = _fetch_case_tab(
                diary_no, diary_year, "judgement_orders"
            )
        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.warning("Failed to fetch SCI judgement/orders: %s", exc)

        orders = _parse_judgement_orders(judgement_html) if judgement_html else []

        return {
            "cin_no": cin_no,
            "registration_no": registration_no,
            "filling_no": diary_number,
            "case_no": case_number,
            "registration_date": registration_date,
            "filing_date": filing_date,
            "first_listing_date": verified_on,
            "next_listing_date": listed_on,
            "last_listing_date": None,
            "decision_date": None,
            "court_no": None,
            "disposal_nature": pending_or_disposed,
            "purpose_next": status,
            "case_type": category,
            "pet_name": ", ".join(petitioners) if petitioners else None,
            "res_name": ", ".join(respondents) if respondents else None,
            "petitioner_advocates": ", ".join(petitioner_advocates) if petitioner_advocates else None,
            "respondent_advocates": ", ".join(respondent_advocates) if respondent_advocates else None,
            "judges": ", ".join(judges) if judges else None,
            "bench_name": None,
            "court_name": None,
            "history": None,
            "acts": acts,
            "orders": orders,
            "listing_dates": listings,
            "additional_info": None,
            "original_json": {
                "case_details": case_payload,
                "listing_dates": listing_payload,
                "judgement_orders": judgement_payload,
            },
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


def parse_cause_list_table(soup):
    results = []
    table = soup.find("table")
    if not table:
        return []

    headers = [th.text.strip() for th in table.find_all("th")]
    rows = table.find_all("tr")[1:]

    current_section = None

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue
        
        # Handle header rows (often used for categories like [FRESH (FOR ADMISSION)])
        # These usually have only one cell with colspan or just a large text
        if len(cells) == 1:
            current_section = cells[0].text.strip()
            continue

        row_data = {}
        if current_section:
            row_data['section'] = current_section
            
        for i, cell in enumerate(cells):
            header = headers[i] if i < len(headers) else f"column_{i+1}"
            text = cell.text.strip()
            row_data[header] = text

            link = cell.find("a")
            if link and link.get("href"):
                row_data[f"{header}_url"] = link.get("href")

        results.append(row_data)
    return results


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_get_cause_list(
    listing_date,
    list_type="daily",
    search_by="all_courts",
    causelist_type="Misc. Court",
    msb="both",
    **kwargs
):
    """
    Fetch cause list data from SCI.
    
    Args:
        listing_date (str): Date in DD-MM-YYYY format.
        list_type (str): 'daily' or 'other'. Defaults to 'daily'.
        search_by (str): 'all_courts', 'court', 'judge', 'aor_code', 'party_name'. Defaults to 'all_courts'.
        causelist_type (str): 'Misc. Court', 'Regular Court', etc. Defaults to 'Misc. Court'.
        msb (str): 'main', 'suppli', 'both'. Defaults to 'both'.
        **kwargs: Additional parameters like 'party_name', 'aor_code', 'court', 'judge'.
    """
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_causes",
        "list_type": list_type,
        "search_by": search_by,
        "listing_date": listing_date,
        "causelist_type": causelist_type,
        "msb": msb,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
    }
    data.update(kwargs)
    
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        
        if response.get("success"):
            # Check for resultsHtml first as per cause list response structure
            html_fragment = response.get("data", {}).get("resultsHtml")
            
            # Fallback to standard extraction if resultsHtml is missing/empty
            if not html_fragment:
                html_fragment = _extract_html_fragment(response)

            if not html_fragment:
                return []
            
            soup = BeautifulSoup(html_fragment, "html.parser")
            
            if "No records found" in soup.text:
                return []

            # Use a more generic table parser for cause lists
            if soup.find("table"):
                 return parse_cause_list_table(soup)
            
            return html_fragment # Return raw HTML if no table found
            
        else:
            return []

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


def sci_get_all_cases_for_day(listing_date: str) -> List[Dict[str, Any]]:
    """
    Fetch all cases listed for a specific day by fetching the 'All Courts' cause list PDFs
    and parsing them.
    """
    logger.info(f"Fetching 'All Courts' cause list for {listing_date}...")
    
    # 1. Get list of PDFs
    # search_by="all_courts" returns rows with PDF links
    pdf_rows = sci_get_cause_list(listing_date, search_by="all_courts")
    
    if not pdf_rows:
        logger.warning(f"No cause lists found for {listing_date}")
        return []
        
    all_cases = []
    
    for row in pdf_rows:
        pdf_url = None
        for key, value in row.items():
            if key.endswith("_url") and value:
                pdf_url = value
                break
        
        if not pdf_url:
            continue
            
        if not pdf_url.startswith("http"):
             if pdf_url.startswith("/"):
                 pdf_url = "https://www.sci.gov.in" + pdf_url
             else:
                 pass 

        logger.info(f"Downloading PDF from {pdf_url}...")
        try:
            resp = _session_get(pdf_url)
            resp.raise_for_status()
            
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
                tmp_pdf.write(resp.content)
                tmp_pdf_path = tmp_pdf.name
                
            try:
                # Parse PDF
                entries = sci_parse_cause_list_pdf(tmp_pdf_path)
                logger.info(f"Parsed {len(entries)} entries from PDF.")
                all_cases.extend(entries)
            finally:
                if os.path.exists(tmp_pdf_path):
                    os.remove(tmp_pdf_path)
                    
        except Exception as e:
            logger.error(f"Failed to process PDF {pdf_url}: {e}")
            continue
            
    return all_cases


def _clean_pdf_line(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return ""
    if cleaned.startswith("Page ") and " of " in cleaned:
        return ""
    if "SUPREME COURT OF INDIA" in cleaned:
        return ""
    if "LIST OF MATTERS" in cleaned:
        return ""
    if "SNo. Case No." in cleaned:
        return ""
    if "Petitioner / Respondent" in cleaned:
        return ""
    return cleaned


def _is_vs_line(text: str) -> bool:
    normalized = re.sub(r"\s+", "", (text or "").upper())
    return normalized in {"VERSUS", "VS", "V/S", "VS.", "V.S."}


def _parse_single_sci_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    case_lines = entry.get("case_lines") or []
    party_lines = entry.get("party_lines") or []
    advocate_lines = entry.get("advocate_lines") or []
    raw_lines = entry.get("raw_lines") or []

    # Extract Case Number
    # Pattern e.g., "C.A. No. 9337/2022", "SLP(C) No. 18263/2022"
    case_no_text = " ".join(case_lines).strip()
    # Basic cleanup
    case_no_text = re.sub(r"\s+", " ", case_no_text)
    
    petitioner: Optional[str] = None
    respondent: Optional[str] = None
    
    # Split parties by "Versus"
    party_text = "\n".join(party_lines)
    vs_match = re.search(r"\b(Versus|VS\.?|V/S)\b", party_text, re.IGNORECASE)
    
    if vs_match:
        petitioner = party_text[:vs_match.start()].strip().replace("\n", " ")
        respondent = party_text[vs_match.end():].strip().replace("\n", " ")
    else:
        # Fallback: simple join
        petitioner = party_text.replace("\n", " ")

    pet_advocate = None
    res_advocate = None
    # Advocate column logic is tricky as it lists both. 
    # Usually aligned with parties but hard to separate without layout.
    # We will just join them for now.
    advocate_text = " ".join(advocate_lines).strip()

    return {
        "item_no": entry.get("item_no"),
        "page_no": entry.get("page_no"),
        "case_no": case_no_text, # Raw case text
        "petitioner": petitioner,
        "respondent": respondent,
        "pet_advocate": advocate_text, # Placeholder mixed
        "res_advocate": None,
        "text": "\n".join(raw_lines).strip(),
    }


def sci_parse_cause_list_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Parse SCI cause-list PDF and extract structured entries.
    """
    entries: List[Dict[str, Any]] = []

    with fitz.open(pdf_path) as doc:
        open_entry: Optional[Dict[str, Any]] = None

        for page_idx in range(doc.page_count):
            page = doc[page_idx]
            lines: List[Dict[str, Any]] = []

            for block in page.get_text("dict").get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    x0, y0, _, _ = line.get("bbox", (0.0, 0.0, 0.0, 0.0))
                    # Skip header/footer zones if needed (approximate)
                    if y0 < 50 or y0 > 800: 
                        pass # Adjust as needed based on analysis
                    
                    line_text = "".join(span.get("text", "") for span in line.get("spans", []))
                    cleaned = _clean_pdf_line(line_text)
                    if not cleaned:
                        continue
                    lines.append({"x": float(x0), "y": float(y0), "text": cleaned})

            lines.sort(key=lambda item: (item["y"], item["x"]))
            
            # Start finding entries based on Item No (x < 65)
            # Item numbers are usually integers, sometimes with decimals like 1.1
            
            starts = [
                line for line in lines 
                if line["x"] < 65 and re.match(r"^\d+(\.\d+)?$", line["text"])
            ]
            starts.sort(key=lambda item: item["y"])

            if not starts:
                # Continuation page? Append to open entry
                if open_entry:
                    for line in lines:
                        x = line["x"]
                        txt = line["text"]
                        open_entry["raw_lines"].append(txt)
                        if 65 <= x < 180:
                            open_entry["case_lines"].append(txt)
                        elif 180 <= x < 420:
                            open_entry["party_lines"].append(txt)
                        elif x >= 420:
                            open_entry["advocate_lines"].append(txt)
                continue

            first_start_y = starts[0]["y"]
            
            # Close previous page's open entry if content exists before first new entry
            if open_entry:
                for line in lines:
                    if line["y"] >= first_start_y:
                        continue
                    x = line["x"]
                    txt = line["text"]
                    open_entry["raw_lines"].append(txt)
                    if 65 <= x < 180:
                        open_entry["case_lines"].append(txt)
                    elif 180 <= x < 420:
                        open_entry["party_lines"].append(txt)
                    elif x >= 420:
                        open_entry["advocate_lines"].append(txt)
                
                entries.append(_parse_single_sci_entry(open_entry))
                open_entry = None

            # Process entries on this page
            for idx, start in enumerate(starts):
                y_start = start["y"]
                y_end = starts[idx + 1]["y"] if idx + 1 < len(starts) else float("inf")
                
                segment = {
                    "item_no": start["text"],
                    "page_no": page_idx + 1,
                    "raw_lines": [],
                    "case_lines": [],
                    "party_lines": [],
                    "advocate_lines": [],
                }
                
                for line in lines:
                    # Check if line is within vertical range of this entry
                    # Note: y_end is start of next entry. 
                    # We might grab footer text if not careful, but _clean_pdf_line handles some.
                    if not (y_start <= line["y"] < y_end):
                        continue
                    
                    x = line["x"]
                    txt = line["text"]
                    
                    # Don't add the item number itself to raw lines or specific columns
                    if line is start:
                        continue

                    segment["raw_lines"].append(txt)
                    
                    if 65 <= x < 180:
                        segment["case_lines"].append(txt)
                    elif 180 <= x < 420:
                        segment["party_lines"].append(txt)
                    elif x >= 420:
                        segment["advocate_lines"].append(txt)

                if idx + 1 < len(starts):
                    entries.append(_parse_single_sci_entry(segment))
                else:
                    open_entry = segment

        if open_entry:
            entries.append(_parse_single_sci_entry(open_entry))

    return entries


def _pdf_normalize(text: str) -> str:
    return re.sub(r"[\s\-]+", "", (text or "").upper())


def sci_find_case_entries_in_pdf(pdf_path: str, case_number: str) -> List[Dict[str, Any]]:
    """
    Find cause-list entries that match a case number.
    Replaces old logic with robust full-PDF parsing.
    """
    all_entries = sci_parse_cause_list_pdf(pdf_path)
    target = _pdf_normalize(case_number)
    
    matches = []
    for entry in all_entries:
        # Check in case_no field
        if target in _pdf_normalize(entry.get("case_no", "")):
            matches.append(entry)
        # Fallback: check in full text
        elif target in _pdf_normalize(entry.get("text", "")):
            matches.append(entry)
            
    return matches


def _fetch_order_document(url: str, referer: Optional[str] = None) -> requests.Response:
    headers = {}
    if referer:
        headers["Referer"] = referer
    return _session_get(url, headers=headers)


async def persist_orders_to_storage(
    orders: Optional[List[dict]],
    case_id: Optional[str] = None,
) -> Optional[List[dict]]:
    """
    Upload scraped SCI order documents to storage and update their URLs.
    """
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer="https://www.sci.gov.in/",
    )


if __name__ == "__main__":
    # Test a single court fetch
    print("\nFetching cases for Court 1 on 09-02-2026...")
    cases = sci_get_cause_list("09-02-2026", search_by="court", court="1")
    print(f"Found {len(cases)} cases in Court 1.")
    if cases:
        print("First case snippet:", str(cases[0])[:200])
