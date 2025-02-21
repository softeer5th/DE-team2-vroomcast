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
import boto3, random

logging.basicConfig(level=logging.INFO)  # 로그 레벨 설정
logger = logging.getLogger(__name__)

# BUCKET_NAME = "hmg-5th-crawling-test"

BASE_URL = "https://gall.dcinside.com/board/lists/?id=car_new1"
WAIT_TIME = 2
# 제목만 / 제목+내용
SEARCH_URL_TITLE = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject&s_keyword="
SEARCH_URL_TITLE_AND_CONTENT = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject_memo&s_keyword="  

def convert_date_format(date_str):
    """
    Converts a date string from 'YY.MM.DD' format to 'YY-MM-DD' format.
    
    Args:
        date_str (str): Date string formatted as 'YY.MM.DD'.
    
    Returns:
        str: Date string formatted as 'YY-MM-DD'.
    
    Example:
        >>> convert_date_format("23.02.15")
        '23-02-15'
    """
    year, month, day = date_str.split('.')
    return f"{year}-{month}-{day}"

def is_date_in_range(date_str, start_date_str, end_date_str):
    """
    주어진 날짜 문자열이 시작 및 종료 날짜 범위 내에 있는지 확인합니다.
    
    매개변수:
        date_str (str): 검사할 날짜 문자열. 형식은 '%y.%m.%d' (예: '23.08.17').
        start_date_str (str): 시작 날짜 문자열. dateutil.parser로 파싱 가능한 형식 (예: '2023-08-16').
        end_date_str (str): 종료 날짜 문자열. dateutil.parser로 파싱 가능한 형식 (예: '2023-11-16').
    
    반환:
        bool: date_str의 날짜가 start_date_str와 end_date_str 사이(포함)에 있을 경우 True를,
              형식 오류 등으로 파싱에 실패한 경우 False를 반환합니다.
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
    
    def __init__(self, s_date, e_date, car_id, car_keyword):
        """
        Initialize a DC_crawler instance with given dates, car ID, and search keyword.
        
        Args:
            s_date (str): Start date for crawling posts.
            e_date (str): End date for crawling posts.
            car_id (str): Identifier for the specific car board.
            car_keyword (str): Keyword used to generate the search URL.
        
        Attributes:
            start_date (str): Assigned start date.
            end_date (str): Assigned end date.
            car_id (str): Assigned car identifier.
            keyword (str): Assigned search keyword.
            search_url (str): URL obtained by concatenating SEARCH_URL_TITLE and car_keyword.
        """
        self.start_date = s_date
        self.end_date = e_date
        self.car_id = car_id
        self.keyword = car_keyword
        self.search_url = SEARCH_URL_TITLE + car_keyword
    # Chrome WebDriver 선언, Lambda 적용 시 주석 필히 보고 해제할 것!!!!!
    def _get_driver(self,):
        # 이 path는 로컬 실행 시 주석처리 하세요.
        # chrome_path = "/opt/chrome/chrome-headless-shell-mac-arm64"
        # driver_path = "/opt/chromedriver"   

        """
        Initializes and returns a headless Chrome WebDriver instance.
        
        Configures Chrome WebDriver with options optimized for headless browsing:
        - Activates headless mode.
        - Disables sandbox, GPU, and shared memory usage.
        - Sets a custom user-agent string.
        - Specifies a fixed window size.
        
        Note:
            Local execution might require adjusting binary locations and driver paths.
        
        Returns:
            webdriver.Chrome: A configured Chrome WebDriver ready for use.
        """
        options = webdriver.ChromeOptions()
        # options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
        options.add_argument("--headless")  # Headless 모드
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0")
        # options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
        options.add_argument("--window-size=1920x1080")
        
        # service = Service(executable_path="/opt/chromedriver")
        driver = webdriver.Chrome(
            # service=service, # 로컬 실행 시 주석 처리
            options=options) 
        return driver
    
    def get_entry_point(self, driver:webdriver.Chrome, url):
        """
        Returns the current page URL after performing a date-based search.
        
        Navigates to the specified URL with the provided WebDriver, opens the date picker,
        inputs the target end date, triggers the search, waits for the results to load, and
        retrieves the resulting page URL.
        
        Args:
            driver (webdriver.Chrome): Configured Selenium Chrome WebDriver instance.
            url (str): The URL to open where the search operation will be executed.
        
        Returns:
            str: The URL of the current page after executing the search operation.
        
        Raises:
            selenium.common.exceptions.TimeoutException: If any expected page element fails to
                become clickable or present within the allotted time.
        """
        s_date = self.start_date
        e_date = self.end_date
        
        driver.get(url)
        time.sleep(WAIT_TIME)
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
        
    def crawl_post_link(self, soup:BeautifulSoup, cur_date:str):
        """
        Extracts and collects post links from the provided page HTML based on a configured date range.
        
        Iterates through rows representing posts, verifies each post's date against the start and end dates,
        and appends valid post details (URL, ID, and date) to the crawler's post_link list. Logs whenever the
        current processing date changes, and terminates further processing by returning False if a post's date
        falls outside the allowed range.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content containing post entries.
            cur_date (str): Reference date for grouping posts during processing.
        
        Returns:
            str or bool: Returns the last processed post date as a string if every post is within the date range;
            returns False immediately upon encountering a post with an out-of-range date.
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
                logger.info(f"Gathering Links of {date}")
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
        return date
    
    def page_traveler(self, driver:webdriver.Chrome, current_link:str):
        """
        Traverse pagination boxes in reverse chronological order.
        
        Navigates through paginated pages by loading each page using the provided
        URL, extracting post links within the specified date range, and advancing to
        the next page based on the pagination controls. A random delay is applied
        between page loads to mimic human browsing behavior. Iteration stops once
        no valid post date is encountered.
        
        Args:
            driver (webdriver.Chrome): The Selenium WebDriver instance used for navigation.
            current_link (str): The URL of the current pagination page to start traversing.
        
        Returns:
            None.
        """
        random_sleep_time = [0.8, 0.6, 0.7, 0.5]
        cur_date = self.end_date
        i = 0
        
        while True:
            driver.get(current_link)
            time.sleep(WAIT_TIME - 1)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # is_crawl_post_success = False
            date = self.crawl_post_link(soup, cur_date)
            
            if date: # 유효하지 않은 날짜를 만날 때 까지 크롤링
                # 한 페이지를 다 긁었으면...
                current_page = soup.select_one('.bottom_paging_box.iconpaging em')
                dc_url = "https://gall.dcinside.com"
                next_link = current_page.find_next_sibling('a')
                current_link = dc_url + next_link['href']
                
                if str(next_link.get('class')) == "search_next": 
                    logger.info("Search next 10000 posts")
                
                # time.sleep(random_sleep_time[i := i % 4])
                time.sleep(random.randrange(500, 1000) / 1000)
                i += 1
            
                cur_date = date    
                
            else: # 특정 범위의 날짜를 전부 크롤링 했다면
                logger.info(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                break
        return
    
    def get_html_of_post(self, url:str):
        """
        Retrieves the HTML content of a forum post.
        
        Performs an HTTP GET request on the specified URL using a custom User-Agent header and retries up to MAX_TRY times
        if a successful response (status code 200) is not received. Logs errors and waits RETRY_WAITS seconds between attempts.
        
        Args:
            url (str): The URL of the post to fetch.
        
        Returns:
            str: The HTML source of the post if the request is successful.
            bool: False if all retry attempts fail.
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
        """
        Saves HTML content and associated post information to a JSON file.
        
        Constructs a file path using the car identifier, the post's date (formatted via convert_date_format),
        and the post's ID, then saves the HTML content along with the post URL in a structured JSON file.
        If the target directories do not exist, they are created. Any errors during file writing are caught
        and printed.
         
        Args:
            html_source (str): HTML content of the post.
            post_info (dict): Dictionary containing post details with the following keys:
                - 'date': The date string of the post.
                - 'id': The unique identifier of the post.
                - 'url': The URL of the post.
        """
        file_path = f"extracted/{self.car_id}/{convert_date_format(post_info['date'])}/raw/dcinside/{post_info['id']}.json"
        directory = os.path.dirname(file_path)
        
        json_body = {
            "url" : post_info['url'],
            "html" : html_source
        }
        
        if not os.path.exists(directory):  # 디렉토리가 존재하지 않으면
            os.makedirs(directory)  # 디렉토리 생성
        
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                # file.write(html_source)
                json.dump(json_body, file, ensure_ascii=False, indent=4)
            # print(f"HTML 소스가 {file_path}에 저장되었습니다.")
            
        except Exception as e:
            print(f"❌ 파일 저장 중 오류 발생: {e}")            
            
    def run_crawl(self,):
        # 드라이버 세팅
        """
        Orchestrates the complete crawling process to collect and save post HTML.
        
        Initializes the Selenium WebDriver in headless mode, navigates to the search URL to
        access the latest posts within the specified date range, and iterates through paginated
        results to gather valid post links. For each collected post, the HTML content is fetched
        with retry logic and saved to a JSON file. A dynamic delay is inserted between requests to
        avoid overloading the server.
        
        Returns:
            None.
        """
        driver=self._get_driver()
        logger.info("✅ Driver Successfully Set.")
        
        # 검색 기간 내 가장 최신 게시글 검색 결과 접근
        end_point = self.get_entry_point(driver, url=self.search_url)
        logger.info("✅ Successfully accessed to init date")
        
        # 접근 위치로부터 거슬러 올라가며 게시글 링크 수집
        self.page_traveler(driver, end_point)
        
        # 수집된 링크를 방문하며 html 소스 저장
        for i, post in enumerate(self.post_link):
            # print(f"Progressing...")
            
            # random_sleep_time = [0.8, 0.6, 0.7, 0.5]
            html_source = self.get_html_of_post(post['url'])
            
            logger.info(f"Saving... [{i+1} / {len(self.post_link)}]")
            self.save_html(html_source, post)
                
            # time.sleep(random_sleep_time[i % 4])
            time.sleep(1 + random.randrange(500, 1000) / 1000)
                    

    
if __name__=="__main__":
    
    car = {'산타페': [ # 차종
                '산타페', # 해당 차종의 이명
                '싼타페']
        }   
  
    s_date="2023-08-16"
    e_date="2023-11-16"
    
    logger.info(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    
    # car_keyword는 lambda_handler에서 event로 처리하게 할 것
    crawler = DC_crawler(s_date, e_date, car_id="santafe", car_keyword="싼타페")
    
    logger.info("Running crawler")
    crawler.run_crawl()
    logger.info("✅ Crawling Finished")
    