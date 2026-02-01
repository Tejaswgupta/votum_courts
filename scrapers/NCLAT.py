import requests
import re
from urllib import parse
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import logging
from rich import print

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PAGE_URL = 'https://nclat.nic.in/display-board/cases'
DATA_URL = 'https://nclat.nic.in/display-board/cases_details'
DETAILS_URL = 'https://nclat.nic.in/display-board/view_details'

session = requests.Session()
session.verify = False
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://nclat.nic.in',
    'Connection': 'keep-alive',
    'Referer': 'https://nclat.nic.in/display-board/cases',
    'Cookie': 'laravel_session=eyJpdiI6ImlTNDkzclBYa3R4dXRKWU5LOUtoQmc9PSIsInZhbHVlIjoiZHRWczNFcVFNcVVTTFFXeE53M091WWxZY2FuSG1zc1B2UjFmUE9DdmVKUW5ZbEEzVUZSTVF1cm9SQUZYN0NGbUZQR2t1Z0xBV2d1OVhtS0dRTlZKSEtEd0VhUEcrV2FRQWlpMmVZMDlDaXBqeEJWeFRMTFRsL3M1RVFrMy9tN3giLCJtYWMiOiIwMGYxZTJhN2JiNzdkZDZlMGQ0ZjBhMjU4MDkzNGRkMmFmM2YwNWFlMDM2ZDY2NjIxZWYwMTQwNzA1ZTllMjVhIiwidGFnIjoiIn0%3D; XSRF-TOKEN=eyJpdiI6IlRHTHJWR01RMU9rb2pxUWlsT3ZMSmc9PSIsInZhbHVlIjoiUWRrNHJpemFGaWU5UGRzQ1h0ZEFkMTYyZlJtd1dDZVAxdWd5L2gxc0dUR1FOZG9HcCtyT050bFlvZUw0eUpMcmtzdVpZWEpQZHVxdzdQVVBqOHJ1MG9SVlQvWTBQVmo2VG1EaGZuaDhLeTdkcDhGTDV5dHlTb3pmaVlLL1ZPc0IiLCJtYWMiOiJlMGE1Njg2ZWZlNWFjNjk2M2U5YzRhM2EyNWNhMmY3ZmVlNDdiMzA5YzNlMjU0NzY2ZmY4MTdkNmJjYmI2MDlkIiwidGFnIjoiIn0%3D',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=1'
}

def get_csrf_token():
    response = session.get(PAGE_URL).text
    csrf_token = response.split('_token" value="')[1].split('"')[0]
    return csrf_token

def response_to_dict(resp):
    columns = ['Sr. No', 'Filing No.', 'Case No.', 'Case Title', 'Registration Date', 'Status']
    result = []
    for row in resp['data']:
        data = dict(zip(columns, row[:-1]))
        data['Status'] = re.sub(r'<.*?>', '', data['Status'])
        result.append(data)
    return [{
        'cino': item['Filing No.'],
        'date_of_decision': item['Registration Date'],
        'pet_name': item['Case Title'].split('VS')[0].strip() if 'VS' in item['Case Title'] else None,
        'res_name': item['Case Title'].split('VS')[1].strip() if 'VS' in item['Case Title'] else None,
        'type_name':  item['Case No.'].split('-')[0],
    } for item in result]

request_data = {
    '_token': '',
    'search_by': '',
    'location': '',
    'case_type': '',
    'exact_search_word': '1',
    'text_name': '',
    'from_date': '',
    'to_date': '',
    'case_status': '',
    'select_party': '',
    'party_name': '',
    'diary_no': '',
    'case_number': '',
    'advocate_name': '',
    'case_year': 'All'
}

def response_to_dict(resp):
    
    return [{
        'cino': item[1],
        'date_of_decision': item[4],
        'pet_name': item[3].split("VS")[0],
        'res_name': item[3].split("VS")[1],
        'type_name': item[2],  } for item in resp['data'] ]
    
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclat_search_by_case_no(location, case_type, case_no, case_year):
    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_by'] = 'case_no_wise'
        data['exact_search_word'] = '1'
        data['location'] = location
        data['case_type'] = case_type
        data['case_number'] = case_no
        data['case_year'] = case_year
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).json()
        return response_to_dict(response)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclat_search_by_free_text(location, search_by, free_text, from_date, to_date):
    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_by'] = 'free_text_wise'
        data['location'] = location
        data['exact_search_word'] = search_by
        data['text_name'] = free_text
        data['from_date'] = from_date
        data['to_date'] = to_date
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).json()
        return response_to_dict(response)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)

def nclat_search_by_filing_number(location, filing_no):

    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_by'] = 'filing_no_wise'
        data['location'] = location
        data['diary_no'] = filing_no
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).json()
        return response_to_dict(response)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclat_search_by_case_type(location, case_type, status):
    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_by'] = 'case_type_wise'
        data['location'] = location
        data['case_type'] = case_type
        data['case_status'] = status
        data['diary_no'] = '111111111111'
        data['case_year'] = 'All'
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).json()
        return response_to_dict(response)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclat_search_by_party_name(location, party_type, party_name):
    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_by'] = 'party_wise'
        data['location'] = location
        data['select_party'] = party_type
        data['party_name'] = party_name
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).json()
        return response_to_dict(response)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclat_search_by_advocate_name(location, representative_name):
    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_by'] = 'advocate_wise'
        data['location'] = location
        data['advocate_name'] = representative_name
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).json()
        
        return response_to_dict(response)


    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def nclat_get_details(filing_no, bench):
    try:
        csrf_token = get_csrf_token()
        print('Start fetching data...')
        data = request_data.copy()
        data['_token'] = csrf_token
        data['search_type'] = 'view_details'
        data['filing_no'] = filing_no
        data['bench_name'] = bench
        data = parse.urlencode(data)
        input_json = session.post(DETAILS_URL, data=data).json()
        data = input_json['data']
       
        output_json = {
    'cin_no': None,
    'registration_no': None,
    'filling_no': data['case_details'][0].get('filing_no') if data.get('case_details') else None,
    'case_no': data['case_details'][0].get('case_no') if data.get('case_details') else None,
    'registration_date': data['case_details'][0].get('registration_date') if data.get('case_details') else None,
    'filing_date': data['case_details'][0].get('date_of_filing') if data.get('case_details') else None,
    'first_listing_date': data['first_hearing_details'].get('hearing_date') if data.get('first_hearing_details') else None,
    'next_listing_date': data['next_hearing_details'].get('hearing_date') if data.get('next_hearing_details') else None,
    'last_listing_date': data['last_hearing_details'].get('hearing_date') if data.get('last_hearing_details') else None,
    'decision_date': None,
    'court_no': data['last_hearing_details'].get('court_no') if data.get('last_hearing_details') else None,
    'disposal_nature': None,
    'purpose_next': data['next_hearing_details'].get('stage_of_case') if data.get('next_hearing_details') else None,
    'case_type': data['case_details'][0].get('case_type') if data.get('case_details') else None,
    'petitioner': [party['name'] for party in data.get('party_details', {}).get('applicant_name', [])],
    'respondent': [party['name'] for party in data.get('party_details', {}).get('respondant_name', [])],
    'petitioner_advocates': data['legal_representative'].get('applicant_legal_representative_name', []),
    'respondent_advocates': data['legal_representative'].get('respondent_legal_representative_name', []),
    'judges': data['last_hearing_details'].get('coram') if data.get('last_hearing_details') else None,
    'bench_name': None,
    'court_name': None,
    'history': data.get('case_history') if data.get('case_history') else None,
    'acts': None,
    'orders': data.get('order_history') if data.get('order_history') else None,
    'additional_info': None,
    'original_json': data
}

        return output_json

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    
if __name__ == '__main__':
    #print(nclat_search_by_case_no('delhi', '33', '1', '2021')) #case type value is 33 for Company Appeal AT
    #print(nclat_search_by_free_text('delhi', '1', 'Gupta', '11/01/2024', '11/26/2024')) #MM/DD/YY
    #print(nclat_search_by_filing_number('delhi', '9910110008502017'))
    #print(nclat_search_by_case_type('delhi', '34', 'P')) #case type value is 33 for Company Appeal AT , 'P' is case status for Pending , 'D' is for dispose and 'all' is for All 
    #print(nclat_search_by_party_name('delhi', '1', 'GUPTA'))
    #print(nclat_search_by_advocate_name('delhi', 'Gupta')[0])
    print(nclat_get_details('9910110000642017', 'delhi'))#9910110000642017 #9910110000062013
    
