import logging

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a reusable session
session = requests.session()
session.verify = False  # Disable SSL verification (use with caution)
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://drt.gov.in/',
    'Origin': 'https://drt.gov.in',
    'Connection': 'keep-alive'
}

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

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def drt_search_by_diary_number(drat, diary_no, diary_year):
    payload = {
        'schemeNameDrtId': drat,
        'diaryNo': diary_no,
        'diaryYear': diary_year
    }
    try:
        resp = session.post(
            url='https://drt.gov.in/drtapi/getDratCaseDetailDiaryNoWise',
            headers=headers,
            data=payload,
            timeout=10  
        )
        data = validate_response(resp)
        return [{
            'cino': data.get('diaryno'),
            'date_of_decision': data.get('dateoffiling'),
            'pet_name': data.get('petitionerName'),
            'res_name': data.get('respondentName'),
            'type_name': data.get('casetype'),
        }]
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def drt_search_by_case_number(drat, case_type, case_no, case_year):
    payload = {
        'schemeNameDrtId': drat,
        'casetype': case_type,
        'caseNo': case_no,
        'caseYear': case_year
    }
    
    try:
        resp = session.post(
            url='https://drt.gov.in/drtapi/getDratCaseDetailCaseNoWise',
            headers=headers,
            data=payload,
            timeout=10
        )
        print(resp.text)
        data = validate_response(resp)
        return [{
            'cino': data.get('diaryno'),
            'date_of_decision': data.get('dateoffiling'),
            'pet_name': data.get("petitionerName"),
            'res_name': data.get("respondentName"),
            'type_name': data.get('casetype'),
        }]
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def drt_search_by_party_name(drat, party_name):
    payload = {
        'schemeNameDratDrtId': drat,
        'partyName': party_name
    }
    try:
        resp = session.post(
            url='https://drt.gov.in/drtapi/drat_party_name_wise',
            headers=headers,
            data=payload,
            timeout=10
        )
        data = validate_response(resp)
        return [{
            'cino': item.get('diaryno'),
            'date_of_decision': item.get('dateoffiling'),
            'pet_name': item.get('applicant'),
            'res_name': item.get('respondent'),
            'type_name': item.get('casetype'),
        } for item in data]
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def drt_get_details(diary_no, diary_year, scheme_name_drt_id):
    payload = {
        'diaryNo': diary_no,
        'diaryYear': diary_year,
        'schemeNameDrtId': scheme_name_drt_id
    }
    try:
        resp = session.post(
            url='https://drt.gov.in/drtapi/getDratCaseDetailDiaryNoWise',
            headers=headers,
            data=payload,
            timeout=10
        )
        original_json = validate_response(resp)
        return {
            "cin_no": original_json.get('diaryno'),
            "registration_no": None,
            "filling_no": None,
            "case_no": original_json.get('caseno'),
            "registration_date": None,
            "filing_date": original_json.get('dateoffiling'),
            "first_listing_date": None,
            "next_listing_date": original_json.get('nextlistingdate'),
            "last_listing_date": None,
            "decision_date": original_json.get('dateofdisposal'),
            "court_no": original_json.get('courtNo'),
            "disposal_nature": original_json.get('disposalNature'),
            "purpose_next": original_json.get('nextListingPurpose'),
            "case_type": original_json.get('casetype'),
            "pet_name": original_json.get('petitionerName'),
            "res_name": original_json.get('respondentName'),
            "petitioner_advocates": {
                "name": original_json.get('advocatePetName'),
                "address": original_json.get('petitionerApplicantAddress')
            },
            "respondent_advocates": {
                "name": original_json.get('advocateResName'),
                "address": original_json.get('respondentDefendentAddress')
            },
            "judges": None,
            "bench_name": None,
            "court_name": original_json.get('courtName'),
            "history": None,
            "acts": None,
            "orders": None,
            "additional_info": None,
            "original_json": original_json
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


if __name__ == '__main__':
    # Example usage
    #print(drt_search_by_diary_number('101', '1111', '2024'))
    #print(drt_search_by_case_number('101', '14', '1111', '2024'))
    print(drt_search_by_party_name('101', 'Gupta'))
    #print(drt_get_details('1111', '2024', '101'))
