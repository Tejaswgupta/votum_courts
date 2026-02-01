import requests
from bs4 import BeautifulSoup
import ddddocr
from urllib import parse
import logging
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ocr = ddddocr.DdddOcr(show_ad=False, beta=True)
session = requests.session()
session.verify = False
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://itat.gov.in/judicial/casestatus',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://itat.gov.in',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Priority': 'u=1'
}

#CAPTCHA_URL = 'https://itat.gov.in/judicial/get_captcha1'
CAPTCHA_URL = 'https://itat.gov.in/captcha/show'
DATA_URL = 'https://itat.gov.in/judicial/casestatus'
CAPTCHA_VERIFY_URL = 'https://itat.gov.in/judicial/check_captcha_js1/'
DETAIL_URL = 'https://itat.gov.in/judicial/judicialdetail'


def get_csrf_token():
    response = session.get(DATA_URL)
    cookies = response.cookies.get_dict()
    csrf_token = cookies.get('csrf_cookie_name')
    return csrf_token

def table_to_list(soup):
    table = []
    for row in soup.find_all('tr'):
        row_data = []
        for cell in row.find_all(['td', 'th']):
            button = cell.find('button')
            if button:
                row_data.append(button.get('value'))
            else:
                row_data.append(cell.get_text().strip())
        if row_data:
            table.append(row_data)
    data = []
    
    for row in table[1:]:
        data.append(dict(zip(table[0], row)))
 
    return [{
        'cino': item['Appeal Number  Assessment Year  Case Status'].split(' Status:')[0],
        'date_of_decision': item['Appeal Number  Assessment Year  Case Status'].split('[')[-1].split(']')[0],  
        'pet_name': item['Parties'].split('VS.')[0].strip(), 
        'res_name': item['Parties'].split('VS.')[1].strip(),  
        'type_name': None
    } for item in data]
     
# @retry(
#     retry=retry_if_exception_type(requests.exceptions.RequestException),
#     stop=stop_after_attempt(5),
#     wait=wait_exponential(multiplier=1, min=2, max=10)
# )

  # data = session.get(CAPTCHA_VERIFY_URL + captcha_result).json()
            # if data['success']:
            #     print('Captcha verified.')
            #     break
            # else:
            #     print('Captcha verification failed. Trying again')
def itat_search_by_appeal_number(bench, appeal_type, number, year):
  
    max_retries = 5 
    retries = 0

    while retries < max_retries:
        try:
            csrf_token = get_csrf_token()
        
            captcha_image = session.get(CAPTCHA_URL).content

            captcha_result = ocr.classification(captcha_image)
         
            data = {
                'csrftkn': csrf_token,
                'c1': captcha_result,
                'bench_name_1': bench,
                'app_type_1': appeal_type,
                'app_number': number,
                'app_year_1': year,
                'bt1': 'true',
            }
            
            data = parse.urlencode(data)

            resp = session.post(DATA_URL, data=data)
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            if resp.text == []:
                print("Retrying...")
                retries += 1
            else:
                print("Data fetched successfully!")
                return table_to_list(soup)

        except Exception as e:
            print(f"An error occurred: {e}")
            retries += 1

    raise Exception("Max retries reached. Could not bypass CAPTCHA.")


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def itat_search_by_filing_date(bench, appeal_type, date_of_filing):
    max_retries = 5
    retries = 0

    csrf_token = get_csrf_token()

    while retries < max_retries:
        try:
            captcha_image = session.get(CAPTCHA_URL).content

            captcha_result = ocr.classification(captcha_image)
            print("Captcha processed successfully.")
            break
            # verify_response = session.get(CAPTCHA_VERIFY_URL + captcha_result).json()

            # if verify_response.get("success"):
            #     print("Captcha verified.")
            #     break  
            # print("Captcha verification failed. Retrying...")
            # retries += 1
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed during captcha handling: {e}")
            retries += 1
            if retries >= max_retries:
                raise RuntimeError("Max retries reached during captcha handling") from e

    if retries >= max_retries:
        print("Captcha verification failed after maximum retries.")
        return None  
    print("Start fetching data...")
    data = {
        "csrftkn": csrf_token,
        "c2": captcha_result,
        "bench_name_2": bench,
        "app_type_2": appeal_type,
        "filed_on": date_of_filing,
        "bt2": "true"}
     
    encoded_data = parse.urlencode(data)

    try:
        resp = session.post(DATA_URL, data=encoded_data)
        resp.raise_for_status()  
        soup = BeautifulSoup(resp.text, "html.parser")
   
        return table_to_list(soup)
    except requests.exceptions.RequestException as e:
        print(f"Data fetching failed: {e}")
        raise

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)  
def itat_search_by_assesee_name(bench, appeal_type, assesee_name):
    csrf_token = get_csrf_token()
    
    while True:
        try:
       
            captcha_image = session.get(CAPTCHA_URL).content
            captcha_result = ocr.classification(captcha_image)
            
            print("Captcha processed successfully.")
            
            print("Start fetching data...")
            data = {
                'csrftkn': csrf_token,
                'c3': captcha_result,
                'bench_name_3': bench,
                'app_type_3': appeal_type,
                'assessee_name': assesee_name,
                'bt3': 'true',
            }
            
            data = parse.urlencode(data)
            resp = session.post(DATA_URL, data=data)
            resp.raise_for_status()  
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            return table_to_list(soup)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)  
def itat_get_details(case_button_id):
    try:
        csrf_token = get_csrf_token()
        data = {
            'csrf_test_name': csrf_token,
            'btnDetail': case_button_id,
        }
        data = parse.urlencode(data)
        html_content = session.post(DETAIL_URL, data=data).text
        soup = BeautifulSoup(html_content, 'html.parser')

        case_details_table = soup.find('table', class_='table table-striped table-bordered manage-efects')
        case_details_rows = case_details_table.find_all('tr')
        appeal_number = case_details_rows[1].find_all('td')[0].text.strip()
        filed_on = case_details_rows[1].find_all('td')[1].text.strip()
        assessment_year = case_details_rows[1].find_all('td')[2].text.strip()
        bench_allotted = case_details_rows[1].find_all('td')[3].text.strip()
        case_status = case_details_rows[1].find_all('td')[4].text.strip()
        appellant = case_details_rows[3].find_all('td')[0].text.strip()
        respondent = case_details_rows[3].find_all('td')[1].text.strip()
        short_summary_table = case_details_table.find_next('table', class_='table table-striped table-bordered')
        short_summary_rows = short_summary_table.find_all('tr')
        date_of_last_hearing = short_summary_rows[0].find_all('td')[0].text.strip()
        date_of_next_hearing = short_summary_rows[0].find_all('td')[1].text.strip()
        orders_table = soup.find_all('table', class_='table table-striped table-bordered')[1]
        order_rows = orders_table.find_all('tr')[1:]
        tribunal_orders = []
        for row in order_rows:
            cells = row.find_all('td')
            if len(cells) == 6:
                order_type = cells[0].text.strip()
                date_of_order = cells[1].text.strip()
                pronounced_on = cells[2].text.strip()
                result = cells[3].text.strip()
                order_link = cells[4].text.strip()
                status_remarks = cells[5].text.strip()
                
                tribunal_orders.append({
                    "order_type": order_type,
                    "date_of_order": date_of_order,
                    "pronounced_on": pronounced_on,
                    "result": result,
                    "order_link": order_link,
                    "status_remarks": status_remarks
                })

        data = {
            "cin_no": None,
            "registration_no": None,
            "filling_no": assessment_year,
            "case_no": appeal_number,
            "registration_date": filed_on,
            "filing_date": filed_on,
            "first_listing_date": date_of_last_hearing,
            "next_listing_date": date_of_next_hearing,
            "last_listing_date": date_of_last_hearing,
            "decision_date": None,
            "court_no": bench_allotted,
            "disposal_nature": case_status,
            "purpose_next": None,
            "case_type": None,
            "pet_name": appellant,
            "res_name": respondent,
            "petitioner_advocates": None,
            "respondent_advocates": None,
            "judges": None,
            "bench_name": None,
            "court_name": None,
            "history": [],
            "acts": None,
            "orders": tribunal_orders,
            "additional_info": None,
            "original_json": None
        }
        return data
    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        
if __name__ == '__main__':
    print(itat_search_by_appeal_number('201', 'ITA', '1111', '2024'))
    ###print(itat_get_details('Njc3OTAx'))
    #print(itat_search_by_filing_date('201', 'ITA', '01/11/2024'))
    ##print(itat_search_by_assesee_name('delhi', 'ITA', 'Patil'))
    # print(get_csrf_token())
    print(itat_search_by_assesee_name('delhi', 'ITA', 'Patil'))
  
    # def itat_search_by_assesee_name1(bench, appeal_type, assesee_name):
    #         csrf_token = get_csrf_token()
        
    #         captcha_image = session.get(CAPTCHA_URL).content

    #         captcha_result = ocr.classification(captcha_image)
         
    #         print(captcha_result)

    #         print("Captcha processed successfully.")
            
    #         print("Start fetching data...")
    #         data = {
    #             'csrftkn': csrf_token,
    #             'c3': '111',
    #             'bench_name_3': bench,
    #             'app_type_3': appeal_type,
    #             'assessee_name': assesee_name,
    #             'bt3': 'true',
    #         }
            
    #         data = parse.urlencode(data)
    #         resp = session.post(DATA_URL, data=data)
    #         resp.raise_for_status()  
            
    #         soup = BeautifulSoup(resp.text, 'html.parser')
    #         table = soup.find('table' , class_ = 'table table-striped table-bordered')
    #         print(table)
            
    
    
    