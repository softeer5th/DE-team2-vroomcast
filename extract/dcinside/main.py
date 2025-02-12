from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager # chrome브라우저 버전에 맞는 드라이버인지 확인 및 없으면 다운로드
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime
from dateutil import parser
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time, json, logging, requests, os
from bs4 import BeautifulSoup
import boto3

logging.basicConfig(level=logging.INFO)  # 로그 레벨 설정
logger = logging.getLogger(__name__)

# BUCKET_NAME = "hmg-5th-crawling-test"

BASE_URL = "https://gall.dcinside.com/board/lists/?id=car_new1"

# 제목만 / 제목+내용
SEARCH_URL_TITLE = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject&s_keyword="
SEARCH_URL_TITLE_AND_CONTENT = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject_memo&s_keyword="  

def convert_date_format(date_str):
    """YY.MM.DD 형식을 YY-MM-DD 형식으로 변환합니다."""
    year, month, day = date_str.split('.')
    return f"{year}-{month}-{day}"

def is_date_in_range(date_str, start_date_str, end_date_str):
    """
    주어진 날짜 문자열이 특정 날짜 범위 안에 있는지 확인합니다 (dateutil 사용).

    Args:
        date_str: 검사할 날짜 문자열 (예: '23.08.17')
        start_date_str: 시작 날짜 문자열 (예: '2023-08-16')
        end_date_str: 종료 날짜 문자열 (예: '2023-11-16')

    Returns:
        bool: 날짜가 범위 안에 있으면 True, 아니면 False
    """
    try:
        # dateutil을 사용하여 날짜 문자열을 datetime 객체로 변환
        date = datetime.strptime(date_str, '%y.%m.%d')
        start_date = parser.parse(start_date_str)
        end_date = parser.parse(end_date_str)

        # 날짜 범위 안에 있는지 확인
        return start_date <= date <= end_date
    except ValueError:
        # logger.error("Error occured while parsing date")
        return False  # 날짜 형식이 잘못된 경우 False 반환
    
class DC_crawler:
    MAX_TRY = 2
    RETRY_WAITS = 2
    post_link = [
    ]
    
    def __init__(self, s_date, e_date, car_id, car_keyword, bucket_name):
        self.start_date = s_date
        self.end_date = e_date
        self.car_id = car_id
        self.keyword = car_keyword
        self.search_url = SEARCH_URL_TITLE + car_keyword
        self.BUCKET_NAME = bucket_name
        self.s3 = boto3.client("s3")
        
    # Chrome WebDriver 선언, Lambda 적용 시 주석 필히 보고 해제할 것!!!!!
    def _get_driver(self,):
        # 이 path는 로컬 실행 시 주석처리 하세요.
        chrome_path = "/opt/chrome/chrome-headless-shell-mac-arm64"
        driver_path = "/opt/chromedriver"   

        options = webdriver.ChromeOptions()
        options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
        options.add_argument("--headless")  # Headless 모드
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
        options.add_argument("--window-size=1920x1080")
        
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(
            service=service, # 로컬 실행 시 주석 처리
            options=options) 
        return driver
    
    def get_entry_point(self, driver:webdriver.Chrome, url):
        s_date = self.start_date
        e_date = self.end_date
        
        driver.get(url)
        
        #-----------------------------------------------
        # 🔹 1. 날짜 검색 창을 여는 버튼 클릭
        #-----------------------------------------------
        open_date_picker = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn_grey_roundbg.btn_schmove")))
        open_date_picker.click()
        time.sleep(1)  # 검색 창이 뜨는 시간 고려
        
        #-----------------------------------------------
        # 🔹 2. 날짜 입력 필드 찾기
        #-----------------------------------------------
        date_input = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input.dayin.calendar")))
        
        #-----------------------------------------------
        # 🔹 3. 날짜 입력
        #-----------------------------------------------
        target_date = e_date  # 검색할 날짜
        # JavaScript로 날짜 값 변경
        driver.execute_script("arguments[0].value = arguments[1];", date_input, target_date)
        date_input.send_keys(target_date)
        date_input.send_keys(Keys.RETURN)  # 엔터 입력

        #-----------------------------------------------
        # 🔹 4. 검색 버튼 클릭
        #-----------------------------------------------
        search_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn_blue.small.fast_move_btn"))
        )
        search_btn.click()

        #-----------------------------------------------
        # 🔹 5. 검색 결과 로딩 대기
        #-----------------------------------------------
        time.sleep(0.5)  # 네트워크 환경에 따라 조정
        
        #-----------------------------------------------
        # 🔹 6. 페이지 소스 가져오기
        #-----------------------------------------------
        current_page_url = driver.current_url
        return current_page_url        
        
    def crawl_post_link(self, driver:webdriver.Chrome, soup:BeautifulSoup, cur_date:str):
        """
        현재 페이지에서 게시글들의 링크를 수집합니다.
        """
        posts = soup.select("tr.ub-content.us-post")
        
        for post in posts:
            # 날짜 검증
            date = post.select_one("td.gall_date").get_text(strip=True) if post.select_one("td.gall_date") else "날짜 없음"
            if not is_date_in_range(date, self.start_date, self.end_date):
                logger.info(f"❗️ Stopped by found date {date}")
                return False
            
            # 날짜 넘어갈 시 로그 작성
            if date != cur_date:
                logger.info(f"Gathering Link of date: {date}")
                cur_date = date
              
            gall_num = int(post.select_one("td.gall_num").get_text(strip=True))
            dc_url = "https://gall.dcinside.com"
            title_tag = post.select_one("td.gall_tit.ub-word a")
            link = dc_url + title_tag["href"] if title_tag else "링크 없음"
            
            post_info = {
                "url" : link,
                "id" : gall_num,
                "date" : date
            }
            
            self.post_link.append(post_info)
        return True, date
    
    def page_traveler(self, driver:webdriver.Chrome, current_link:str):
        """
        페이징 박스를 순회합니다.
        시간 역순으로 순회합니다. 
        (페이징 박스는 정방향 순회, 보이는 게시글은 시간 역순)
        """
        random_sleep_time = [0.8, 0.6, 0.7, 0.5]
        cur_date = self.end_date
        i = 0
        
        while True:
            driver.get(current_link)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            is_crawl_post_success = False
            is_crawl_post_success, date = self.crawl_post_link(driver, soup, cur_date)
            
            if is_crawl_post_success: # 유효하지 않은 날짜를 만날 때 까지 크롤링
                # 한 페이지를 다 긁었으면...
                current_page = soup.select_one('.bottom_paging_box.iconpaging em')
                dc_url = "https://gall.dcinside.com"
                next_link = current_page.find_next_sibling('a')
                current_link = dc_url + next_link['href']
                
                if next_link.__class__ == "search_next": 
                    logger.info("Search next 10000 posts")
                
                time.sleep(random_sleep_time[i := i % 4])
                i += 1
            
                cur_date = date    
                
            else: # 특정 범위의 날짜를 전부 크롤링 했다면
                logger.info(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                break
        return
    
    def get_html_of_post(self, url:str):
        """
        각 게시글의 html source를 가져옵니다.
        가져온 source를 반환합니다.
        """
        headers = {'User-Agent': "Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)"}
        for _ in range(self.MAX_TRY):
            response = requests.get(url, headers=headers)
            
            if response.status_code==200:
                html_source = response.text
                logger.info("Get link OK")
                return html_source
            
            else:# 페이지 접근 재시도
                logger.error(f"❌ {url} request FAILED!")
                time.sleep(self.RETRY_WAITS)
                continue
        return False
            
    def save_html(self, html_source:str, post_info:dict):
        file_path = f"extracted/{self.car_id}/{convert_date_format(post_info['date'])}/raw/dcinside/{post_info['id']}.json"
        directory = os.path.dirname(file_path)
        
        json_body = {
            "url" : post_info['url'],
            "html" : html_source
        }
        
        if not os.path.exists(directory):  # 디렉토리가 존재하지 않으면
            os.makedirs(directory)  # 디렉토리 생성
        
        try:
            # with open(file_path, "w", encoding="utf-8") as file:
                # file.write(html_source)
            web_data = json.dumps(json_body, ensure_ascii=False, indent=4)
            
        except Exception as e:
            print(f"❌ json dump 중 오류 발생: {e}")       
            
        try:
            self.s3.pub_object(
                Bucket = self.BUCKET_NAME,
                Key = f"{post_info['id']}.json",
                Body = web_data,
                ContentType = "application/json"
            )     
            logger.info(f"✅ Successfully uploaded {file_path} to {self.BUCKET_NAME}")

        except Exception as e:
            logger.error(f"❌ Error uploading file to S3: {e}", exc_info=True)
        
    def run_crawl(self,):
        # 드라이버 세팅
        driver=self._get_driver()
        logger.info("✅ Driver Successfully Set.")
        
        # 검색 기간 내 가장 최신 게시글 검색 결과 접근
        end_point = self.get_entry_point(driver, url=self.search_url)
        logger.info("✅ Successfully accessed to init date")
        
        # 접근 위치로부터 거슬러 올라가며 게시글 링크 수집
        self.page_traveler(driver, end_point)
        
        # 수집된 링크를 방문하며 html 소스 저장
        for i, post in enumerate(self.post_link):
            # print(f"Progressing... [{i+1} / {len(self.post_link)}]")
            
            random_sleep_time = [0.8, 0.6, 0.7, 0.5]
            html_source = self.get_html_of_post(post['url'])
            
            logger.info(f"Saving...[{i+1} / {len(self.post_link)}]")
            self.save_html(html_source, post)
                
            time.sleep(random_sleep_time[i % 4])
                    

    
def lambda_handler(event, context):
    BUCKET_NAME = event.get('bucket_name')
    # car = {'산타페': [ # 차종
    #             '산타페', # 해당 차종의 이명
    #             '싼타페']
    #     }   
  
    s_date="2023-11-14"
    e_date="2023-11-16"
    
    logger.info(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    
    # car_keyword는 lambda_handler에서 event로 처리하게 할 것
    crawler = DC_crawler(s_date, e_date, car_id="santafe", car_keyword="산타페", bucket_name=BUCKET_NAME)
    
    logger.info("Running crawler")
    crawler.run_crawl()
    logger.info("✅ Crawling Finished")
    