import requests
from urllib import parse
from bs4 import BeautifulSoup
import ddddocr

import logging
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ocr = ddddocr.DdddOcr(show_ad=False)
session = requests.session()
session.verify = False
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://cestat.gov.in/',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://cestat.gov.in',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Priority': 'u=1'
}

DATA_URL = 'https://cestat.gov.in/casestatus'

def get_csrf_token():
    response = session.get(DATA_URL).text
    csrf_token = response.split('name="csrf_token" value="')[1].split('"')[0]
    return csrf_token

def extract_table_data(soup):
    table = soup.find('table',{'id': 'example'})
    headers = [header.text.strip() for header in table.find_all('th')]
    row_values = []

    for tr in table.find_all('tr'):  
            cells = tr.find_all('td')
            if len(cells) == len(headers): 
                row_dict = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
                row_values.append(row_dict)

    result = [
            {
                'cino': item.get('Diary No', None),
                'date_of_decision': None,
                'pet_name': item.get('Applicant Name', None),
                'res_name': item.get('Respondent Name', None),
                'type_name': None
            }
            for item in row_values
        ]
        
    return result

# def table_to_list(soup):
    
#     table = soup.find('table', class_ ="table table-striped")
#     headers = [header.text.strip() for header in table.find_all('th')]
#     row_values = []

#     for tr in table.find_all('tr')[1:]:  
#         cells = tr.find_all('td')
#         if len(cells) == len(headers):  
#             row_dict = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
#             row_values.append(row_dict)
    
#     result = [
#     {
#         'cino': item.get('Diary No', None),  
#         'date_of_decision': None,        
#         'pet_name': item.get('Applicant Name', None),
#         'res_name': item.get('Respondent Name', None),
#         'type_name': None                
#     }
#     for item in row_values
# ]
#     return result


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def cestat_search_by_diary(diary_num, year, bench):
    try:
        csrf_token = get_csrf_token()
        data = {
            'csrf_token': csrf_token,
            'app_type': 'dno',
            'token_no': diary_num,
            'token_year': year,
            'schema_type': bench,
            'button1': 'SEARCH',
            'captcha_code' : '111111',
        }
        data = parse.urlencode(data)
        print(data)
        response = session.post(DATA_URL, data=data).text
        soup = BeautifulSoup(response, 'html.parser')
        return extract_table_data(soup)

    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def cestat_search_by_case_no(case_type, case_no, year, bench):
    try:
        # response = session.get(DATA_URL)
        # cookies = session.cookies.get_dict()
        # csrf_token = cookies.get('csrf_cookie_name')
        
        csrf_token =  get_csrf_token()
        
        data = {
        'csrf_token': csrf_token,
        'schema_type': bench,
        'app_type': 'cno',
        'case_type': case_type,
        'token_no': case_no,
        'token_year': year,
        'captcha_code': '111111',
        'button2': 'Search'
    }
       
        response = session.post(DATA_URL, data=data)
        soup = BeautifulSoup(response.text, 'html.parser')
        return extract_table_data(soup)
    
    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def cestat_search_by_party_name(party_name, bench):
    try:
        csrf_token = get_csrf_token()
        data = {
            'csrf_token': csrf_token,
            'app_type': 'pno',
            'schema_type': bench,
            'token_no': party_name,
            'button3': 'SEARCH',
            'captcha_code' : 111111,
        }
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).text
        soup = BeautifulSoup(response, 'html.parser')
        return extract_table_data(soup)
    
    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)           
def cestat_search_by_oia(oia, bench):
    try:
        csrf_token = get_csrf_token()
        data = {
            'csrf_token': csrf_token,
            'app_type': 'ino',
            'schema_type': bench,
            'token_no': oia,
            'button4': 'SEARCH',
            'example_length': '10'
        }
        data = parse.urlencode(data)
        response = session.post(DATA_URL, data=data).text
        soup = BeautifulSoup(response, 'html.parser')
        
        return extract_table_data(soup)
    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
) 
def cestat_extract_case_details(diary_number, bench):
    try:
        html_content = session.get(f'https://cestat.gov.in/casedetailreport/{diary_number}/{bench}')
        print(html_content)
        if html_content.status_code != 200:
            return {}
        soup = BeautifulSoup(html_content.text, 'lxml')

        print(soup)
        def get_text_safe(element):
            return element.get_text(strip=True) if element else ''

        tables = soup.find_all('table')

        data = {}

        case_status = {}
        rows = tables[0].find_all('tr')
        rows.extend(tables[1].find_all('tr'))
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 2: 
                key = get_text_safe(cells[0]).replace('.', '')
                value = get_text_safe(cells[1])
                case_status[key] = value
        data['case_status'] = case_status

        case_stage = {}
        rows = tables[2].find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 2:  
                key = get_text_safe(cells[0])
                value = get_text_safe(cells[1])
                case_stage[key] = value
        data['case_stage'] = case_stage

    
        petitioners = {}
        rows = tables[3].find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 2:  
                key = get_text_safe(cells[0])
                value = get_text_safe(cells[1])
                petitioners[key] = value
        data['petitioners'] = petitioners


        respondents = {}
        rows = tables[4].find_all('td')
        respondents[get_text_safe(rows[1])] = get_text_safe(rows[2])
        respondents[get_text_safe(rows[3])] = get_text_safe(rows[4])
        data['respondents'] = respondents

        proceeding_details = []
        rows = tables[5].find_all('td')
        respondents[get_text_safe(rows[1])] = get_text_safe(rows[2])
        respondents[get_text_safe(rows[3])] = get_text_safe(rows[4])
        data['proceeding_details'] = proceeding_details

        application_details = []
        rows = tables[6].find_all('tr')
        headers = [get_text_safe(th) for th in rows[1].find_all('td')]
        for row in rows[2:]:
            cells = row.find_all('td')
            if len(cells) == len(headers):  
                application_detail = {headers[i]: get_text_safe(cells[i]) for i in range(len(headers))}
                application_details.append(application_detail)
            elif len(cells) == len(headers) - 1:
                application_detail = {headers[i]: get_text_safe(cells[i]) for i in range(len(cells))}
                application_detail['Misc Order No'] = None
                application_detail['Misc Order Date'] = None
                application_details.append(application_detail)
        data['application_details'] = application_details

        return {
            "cin_no": None,
            "registration_no": diary_number,
            "filling_no": None,
            "case_no": data.get('case_status', {}).get('Case Type/Case No/Year').split('/')[1],
            "registration_date": None,
            "filing_date": data.get('case_status', {}).get('Date of Filing'),
            "first_listing_date": data['application_details'][0].get('Hearing Date') if len(data['application_details']) else None,
            "next_listing_date": data.get('case_stage', {}).get('Next Hearing Date'),
            "last_listing_date": data.get('case_stage', {}).get('Previous Hearing Date'),
            "decision_date": data.get('case_stage', {}).get('Final Order Date'),
            "court_no": None,
            "disposal_nature": data.get('case_stage', {}).get('Disposal Nature'),
            "purpose_next": data.get('case_stage', {}).get('Next Hearing Purpose'),
            "case_type": data.get('case_status', {}).get('Case Type/Case No/Year').split('/')[0],
            "pet_name": data.get('petitioners', {}).get('Petitioner Name'),
            "res_name": data.get('respondents', {}).get('Respondent Name'),
            "petitioner_advocates": data.get('petitioners', {}).get('Petitioner Advocate'),
            "respondent_advocates": data.get('respondents', {}).get('Respondent Advocate'),
            "judges": None,
            "bench_name": data.get('case_stage', {}).get('Bench type'),
            "court_name": None,
            "history": data.get('proceeding_details'),
            "acts": None,
            "orders": None,
            "additional_info": None,
            "original_json": data
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

if __name__ == '__main__':
    #print(cestat_search_by_diary('000512', '2011', 'delhi'))
    #print(cestat_search_by_case_no('3', '55838', '2013', 'delhi'))
    item = cestat_search_by_party_name('Gupta', 'delhi')
    print(item)
    ##print(cestat_extract_case_details("051607", "delhi"))
    #print(cestat_search_by_oia('31-COMMR-MRT-I-2011', 'delhi'))
