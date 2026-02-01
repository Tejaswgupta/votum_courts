import json
import logging
import re
from urllib import parse

import requests
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from ..order_storage import \
    persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

BASE_URL = 'https://efiling.nclt.gov.in/'
SEARCH_URL = 'https://efiling.nclt.gov.in/caseHistoryoptional.drt'
DETAILS_URL = 'https://efiling.nclt.gov.in/caseHistoryalldetails.drt'
ORDERS_URL = 'https://efiling.nclt.gov.in/ordersview.drt'

session = requests.Session()
session.verify = False
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/json',
    'Origin': 'https://efiling.nclt.gov.in',
    'Referer': 'https://efiling.nclt.gov.in/casehistorybeforeloginmenutrue.drt',
    'X-Requested-With': 'XMLHttpRequest',
}

BENCH_MAP = {
    'principal': '10',
    'new delhi': '10',
    'delhi': '10',
    'mumbai': '9',
    'cuttack': '13',
    'ahmedabad': '1',
    'amaravati': '12',
    'chandigarh': '4',
    'kolkata': '8',
    'jaipur': '11',
    'bengaluru': '3',
    'bangalore': '3',
    'chennai': '5',
    'guwahati': '6',
    'hyderabad': '7',
    'kochi': '14',
    'indore': '15',
    'allahabad': '2',
    'prayagraj': '2',
}

def get_bench_id(bench_name):
    if not bench_name:
        return '0'
    normalized = bench_name.lower().strip()
    # Check for direct match or partial match
    for key, val in BENCH_MAP.items():
        if key in normalized:
            return val
    return '0'

def _standardize_result(item):
    """
    Standardize the result from the search list to match the expected output format.
    """
    return {
        'cino': item.get('filling_no'), # Using Case No as CINO for now, as usually CINO is unique but here Case No is prominent
        'date_of_decision': item.get('date_of_filing'),
        'pet_name': item.get('case_title1'),
        'res_name': item.get('case_title2'),
        'type_name': item.get('status'),
        'filing_no': item.get('filing_no'), # Important for fetching details
        'case_no': item.get('case_no'),
        'bench': item.get('bench_location_name')
    }

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclt_search_by_filing_number(bench, filing_number):
    # Note: filing_year is not explicitly used in the filing number search payload of the new site,
    # but the old signature included it. We'll ignore it or check if it's part of filing_number.
    try:
        payload = {
            "wayofselection": "filingnumber",
            "i_bench_id": get_bench_id(bench),
            "filing_no": filing_number
        }
        resp = session.post(SEARCH_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()
        
        if 'mainpanellist' in data and data['mainpanellist']:
            return [_standardize_result(item) for item in data['mainpanellist']]
        return []

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclt_search_by_case_number(bench, case_type, case_number, case_year):
    try:
        payload = {
            "wayofselection": "casenumber",
            "i_bench_id_case_no": get_bench_id(bench),
            "i_case_type_caseno": case_type, # Expecting ID
            "case_no": case_number,
            "i_case_year_caseno": case_year
        }
        resp = session.post(SEARCH_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()
        
        if 'mainpanellist' in data and data['mainpanellist']:
            return [_standardize_result(item) for item in data['mainpanellist']]
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclt_search_by_party_name(bench, party_type, party_name, case_year, case_status):
    try:
        payload = {
            "wayofselection": "partyname",
            "i_bench_id_party": get_bench_id(bench),
            "party_type_party": party_type, # 'P' or 'R' or '0'
            "party_name_party": party_name,
            "i_case_year_party": case_year,
            "status_party": case_status, # 'P' or 'D' or '0'
            "i_party_search": "E" # Default to Exact, maybe 'W' for wrap?
        }
        resp = session.post(SEARCH_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()
        
        if 'mainpanellist' in data and data['mainpanellist']:
            return [_standardize_result(item) for item in data['mainpanellist']]
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclt_search_by_advocate_name(bench, advocate_name, year):
    try:
        payload = {
            "wayofselection": "advocatename",
            "i_bench_id_lawyer": get_bench_id(bench),
            "party_lawer_name": advocate_name,
            "i_case_year_lawyer": year,
            "bar_council_advocate": "", # Optional
            "i_adv_search": "E"
        }
        resp = session.post(SEARCH_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()
        
        if 'mainpanellist' in data and data['mainpanellist']:
            return [_standardize_result(item) for item in data['mainpanellist']]
        return []

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclt_get_details(bench, filing_no):
    # Bench argument is preserved for compatibility but not strictly needed for the detail fetch 
    # as filing_no is unique global identifier in NCLT usually, or at least the API just needs filing_no.
    try:
        params = {
            'filing_no': filing_no,
            'flagIA': 'false'
        }
        # The endpoint expects GET
        resp = session.get(DETAILS_URL, params=params)
        resp.raise_for_status()
        data = resp.json() # It returns JSON

        # Parse detailed data
        # Structure is complex, we need to map it to the expected output format
        
        # 1. Registration Info
        reg_list = data.get('isregistered') or []
        reg_info = reg_list[0] if reg_list else {}
        case_no = reg_info.get('case_no')
        reg_date = reg_info.get('regis_date')
        
        # 2. Party Details
        parties = data.get('partydetailslist') or []
        pet_names = []
        res_names = []
        pet_advs = []
        res_advs = []
        
        for p in parties:
            ptype = p.get('party_type', '').strip().upper()
            name = p.get('party_name', '').strip()
            adv = p.get('party_lawer_name', '').strip()
            
            # Check for Petitioner/Applicant (P, A, P1, A1, Petitioner, Applicant)
            if ptype.startswith('P') or ptype.startswith('A') or 'PETITIONER' in ptype or 'APPLICANT' in ptype:
                if name: pet_names.append(name)
                if adv and adv.upper() != 'NA': 
                    # Split multiple advocates if comma separated
                    for a in adv.split(','):
                        a = a.strip()
                        if a: pet_advs.append(a)
            
            # Check for Respondent (R, R1, Respondent)
            elif ptype.startswith('R') or 'RESPONDENT' in ptype:
                if name: res_names.append(name)
                if adv and adv.upper() != 'NA':
                    for a in adv.split(','):
                        a = a.strip()
                        if a: res_advs.append(a)
        
        # 3. Status
        final_status_list = data.get('allfinalstatuslist') or []
        final_status = final_status_list[0] if final_status_list else {}
        case_status = final_status.get('current_status')
        listing_date = final_status.get('listing_date')
        
        # 4. Orders
        orders = []
        proceedings = data.get('allproceedingdtls') or []
        for proc in proceedings:
            order_path = proc.get('encPath') # This is the 'path' param for ordersview.drt
            # Construct URL: https://efiling.nclt.gov.in/ordersview.drt?path={encPath}
            order_url = f"{ORDERS_URL}?path={parse.quote(order_path)}" if order_path and order_path != 'NA' else None
            
            orders.append({
                "date": proc.get('order_upload_date') or proc.get('listing_date'),
                "description": f"Listing: {proc.get('listing_date')} | Purpose: {proc.get('purpose')} | Action: {proc.get('today_action')}",
                "document_url": order_url,
                "source_document_url": order_url,
                "listing_date": proc.get('listing_date'),
                "upload_date": proc.get('order_upload_date')
            })
            
        # 5. Connected Matters (using IA/MA list or similar)
        connected = []
        ias = data.get('mainFilnowithIaNoList') or []
        for ia in ias:
            connected.append({
                "filing_no": ia.get('filing_no'),
                "case_no": ia.get('case_no'),
                "title": f"{ia.get('case_title1')} VS {ia.get('case_title2')}",
                "status": ia.get('status')
            })
            
        # Construct result
        return {
            "cin_no": filing_no, # Using filing_no as cin_no for now
            "registration_no": reg_info.get('registration_no'), # or case_no?
            "filling_no": filing_no,
            "case_no": case_no,
            "registration_date": reg_date,
            "filing_date": final_status.get('date_of_filing'),
            "first_listing_date": listing_date,
            "next_listing_date": None, # Could extract from proceedings
            "last_listing_date": listing_date,
            "decision_date": None,
            "court_no": final_status.get('court_no'),
            "disposal_nature": None,
            "purpose_next": None,
            "case_type": final_status.get('case_type'),
            "pet_name": pet_names,
            "res_name": res_names,
            "petitioner_advocates": list(set(pet_advs)),
            "respondent_advocates": list(set(res_advs)),
            "judges": None,
            "bench_name": bench,
            "court_name": final_status.get('bench_nature_descr'),
            "history": [], # Detailed history is in orders/proceedings
            "acts": None,
            "orders": orders,
            "additional_info": {
                "case_status": case_status,
                "party_name": f"{', '.join(pet_names)} VS {', '.join(res_names)}",
                "listing_history": proceedings,
                "ia_ma": ias,
                "connected_matters": connected,
            },
            "original_json": data,
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

def _fetch_order_document(order_url: str, referer: str | None):
    headers = session.headers.copy()
    if referer:
        headers["Referer"] = referer
    return session.get(order_url, timeout=30, headers=headers)

async def persist_orders_to_storage(
    orders: list[dict] | None,
    case_id: str | None = None,
) -> list[dict] | None:
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        base_url=BASE_URL,
        referer=f"{BASE_URL}caseHistoryalldetails.drt",
    )

if __name__ == '__main__':
    # Test code
    # NOTE: You need valid IDs for bench and case types for this to work.
    # Case Type 16 is "Company Petition IB(IBC)"
    
    print(nclt_search_by_case_number('ahmedabad', '14', '1', '2026')) 
    print(json.dumps(nclt_get_details('ahmedabad', '2401105033432025'))) # Use a valid filing number found from search
    pass