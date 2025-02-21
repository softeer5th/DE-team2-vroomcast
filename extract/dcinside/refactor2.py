from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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


BASE_URL = "https://gall.dcinside.com/board/lists/?id=car_new1"
WAIT_TIME = 2
# 제목만 / 제목+내용
SEARCH_URL_TITLE = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject&s_keyword="
SEARCH_URL_TITLE_AND_CONTENT = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject_memo&s_keyword="  

def convert_date_format(date_str):
    # date_str = str(md_to_ymd(date_str))
    """
    Converts a date string to a standardized date format.
    
    Extracts the date portion (ignoring any time component) from the input string and
    replaces periods ('.') with hyphens ('-'). For example, "2024.10.30 12:00:00" becomes "2024-10-30".
    
    Args:
        date_str (str): The input date string, potentially including time data.
    
    Returns:
        str: The date portion of the input string with periods replaced by hyphens.
    """
    return str(date_str).split(' ')[0].replace('.', '-')

def md_to_ymd(date_str:str):
    """
    입력된 날짜 문자열을 "yyyy.mm.dd HH:MM:SS" 형식으로 변환합니다.
    
    지원하는 날짜 형식:
      - "yyyy.mm.dd HH:MM:SS": 변환 없이 원본 문자열 그대로 반환.
      - "mm.dd HH:MM:SS": 현재 연도를 적용하여 "yyyy.mm.dd HH:MM:SS" 형식으로 변환.
    
    Args:
        date_str (str): 변환할 날짜 문자열. 지원 형식은 "yyyy.mm.dd HH:MM:SS" 또는 "mm.dd HH:MM:SS"입니다.
    
    Returns:
        str: "yyyy.mm.dd HH:MM:SS" 형식의 날짜 문자열. 입력 형식이 지원되지 않을 경우 "Invalid date format"를 반환.
    
    Examples:
        >>> md_to_ymd("2024.12.31 23:59:59")
        "2024.12.31 23:59:59"
        >>> md_to_ymd("12.31 23:59:59")
        "2025.12.31 23:59:59"
    """
    try:
        # "yyyy.mm.dd HH:MM:SS" 형식인 경우 그대로 반환
        datetime.strptime(date_str, "%Y.%m.%d %H:%M:%S")
        return date_str
    except ValueError:
        try:
            # "mm.dd HH:MM:SS" 형식인 경우 연도를 2025로 가정하여 변환
            date_obj = datetime.strptime(date_str, "%m.%d %H:%M:%S")
            return date_obj.replace(year=datetime.now().year).strftime("%Y.%m.%d %H:%M:%S")
        except ValueError:
            return "Invalid date format"

def date_type_for_search_result(date_str):
    """
    Parse a date string into a datetime object using supported formats.
    
    Attempts to interpret the input as "mm.dd" (with the current year) or "yy.mm.dd".
    Returns a datetime object if the string matches one of these formats; otherwise,
    returns the string "Invalid Date Format".
    
    Args:
        date_str (str): Date string in "mm.dd" or "yy.mm.dd" format.
    
    Returns:
        datetime.datetime or str: A datetime object if parsing is successful,
        or "Invalid Date Format" if neither format is matched.
    """
    try:
        # 'mm.dd' 형식 처리
        date_obj = datetime.strptime(date_str, "%m.%d")
        date_obj = date_obj.replace(year=datetime.now().year)
        # print(date_obj)
        return date_obj
    except ValueError:
        try:
            # 'yy.mm.dd' 형식 처리
            date_obj = datetime.strptime(date_str, "%y.%m.%d")
            # print(date_obj)
            return(date_obj)
        except ValueError:
            return "Invalid Date Format"

def is_date_in_range(date, start_date_str, end_date_str):
    """
    Verifies if a given date falls within the specified range.
    
    Parses the start and end dates from strings formatted as "YYYY-MM-DD" and
    checks whether the provided date (a datetime object) is between these dates,
    inclusive.
    
    Args:
        date (datetime.datetime): Date to validate.
        start_date_str (str): Start date as a string in "YYYY-MM-DD" format.
        end_date_str (str): End date as a string in "YYYY-MM-DD" format.
    
    Returns:
        bool: True if the date is within the range; False if it is outside the range.
        str: "Invalid Date Format" if either start_date_str or end_date_str cannot be parsed.
    """


    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        return start_date <= date <= end_date

    except ValueError:
        return "Invalid Date Format"
    
def is_time_in_range(time_str, batch_time):
  """
  Checks whether a given time is within the range from six hours before to the reference time.
  
  Args:
      time_str (str): A time string in the format "%Y-%m-%d %H:%M:%S" representing the time to check.
      batch_time (str): A reference time string in the format "%Y-%m-%d %H:%M:%S" used as the upper bound.
  
  Returns:
      bool: True if time_str is between (batch_time - 6 hours) and batch_time, False otherwise.
  """

  try:
    input_time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
  except ValueError:
    return False  # 잘못된 형식의 문자열

  now = datetime.datetime.strptime(batch_time, "%Y-%m-%d %H:%M:%S")
  six_hours_ago = now - datetime.timedelta(hours=6)
  print(now)
  print(input_time, "<-- Input time")
  print(six_hours_ago)

  return six_hours_ago <= input_time <= now    
class DC_crawler:
    MAX_TRY = 2
    RETRY_WAITS = 2
    post_link = [
    ]
    
    def __init__(self, s_date, e_date, car_id, car_keyword, is_daily_batch):
        """
        Initializes the DC_crawler instance with search parameters.
        
        Args:
            s_date (str): The desired start date for crawling (e.g., "yyyy-mm-dd").
            e_date (str): The desired end date for crawling (e.g., "yyyy-mm-dd").
            car_id (str): Identifier for the target car.
            car_keyword (str): Keyword related to the car to build the search URL.
            is_daily_batch (bool): If True, overrides s_date and e_date to the current date.
        
        Side Effects:
            Constructs the search URL by concatenating a global base URL with the car_keyword.
            When is_daily_batch is True, sets both start_date and end_date to the current date.
        """
        self.start_date = s_date
        self.end_date = e_date
        self.car_id = car_id
        self.keyword = car_keyword
        self.search_url = SEARCH_URL_TITLE + car_keyword
        if is_daily_batch: 
            self.daily_batch = datetime.now().strftime("%Y-%m-%d")
            self.start_date = daily_batch
            self.end_date = daily_batch
    # Chrome WebDriver 선언, Lambda 적용 시 주석 필히 보고 해제할 것!!!!!
    def _get_driver(self,):
        # 이 path는 로컬 실행 시 주석처리 하세요.
        # chrome_path = "/opt/chrome/chrome-headless-shell-mac-arm64"
        # driver_path = "/opt/chromedriver"   

        """
        Configures and returns a headless Chrome WebDriver with custom options.
        
        Sets up a Selenium Chrome WebDriver using ChromeOptions for headless operation, no sandbox,
        disabled GPU, a specific user-agent, and a fixed window size. Commented sections indicate alternative
        configurations for local execution.
            
        Returns:
            webdriver.Chrome: A headless Chrome WebDriver instance initialized with the specified options.
        """
        options = webdriver.ChromeOptions()
        # options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
        options.add_argument("--headless")  # Headless 모드
        options.add_argument("--no-sandbox")
        # options.add_argument("--disable-dev-shm-usage")
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
        Navigates to the target URL and applies a date filter to load search results.
        
        Opens the webpage provided by the URL, clicks the date picker button, and waits
        for its appearance. Sets the search date using the crawler's end_date attribute,
        submits the date using JavaScript and keystrokes, and then clicks the search
        button. After a short wait for results to load, returns the URL of the current
        page containing the search results.
        
        Args:
            driver (webdriver.Chrome): Selenium Chrome WebDriver instance.
            url (str): The URL to navigate to for initiating the date filter search.
        
        Returns:
            str: The URL of the loaded page after executing the search action.
        
        Raises:
            TimeoutException: If essential elements (date picker, date input, or search
                              button) are not found within the specified wait durations.
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
        Extracts and processes post links from the current page's HTML.
        
        Scans the provided BeautifulSoup object for post entries identified by the "tr.ub-content.us-post" selector.
        For each post, the method extracts the date from the designated element and converts it using a helper function.
        It then checks whether the date falls within the allowed range defined by the crawler's start_date and end_date.
        If a post's date is out of range, the method logs the event and returns False immediately.
        Otherwise, it updates the current date if a new date is encountered, extracts the post's unique ID and URL,
        constructs a dictionary with these details, and appends it to the crawler's post_link list.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content of the current page.
            cur_date (str): The current date string used for tracking and logging date transitions.
        
        Returns:
            Union[str, bool]: The last processed post date (formatted as a string) if posts are collected successfully,
            or False if a post's date is found to be out of the specified range.
        """
        posts = soup.select("tr.ub-content.us-post")
        
        for post in posts:
            # 날짜 검증
            date = post.select_one("td.gall_date")['title'] if post.select_one("td.gall_date") else "날짜 없음"
            date = date_type_for_search_result(date)
            
            if not is_date_in_range(date, self.start_date, self.end_date):
                logger.info(f"❗️ Stopped by found date {str(date).split()[0]}")
                return False
            
            date = str(date).split()[0]
            
            # 날짜 넘어갈 시 로그 작성
            if date != cur_date:
                logger.info(f"🔗 of {date}")
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
        Iterates through pagination pages to collect posts within the specified date range.
        
        Navigates the pages by loading the current link, parsing the HTML for post dates,
        and then locating the next page link. The process continues until the posts fall
        outside the desired date interval (from self.start_date to self.end_date). Random
        delays between page loads help mimic natural browsing.
        
        Parameters:
            driver (webdriver.Chrome): Selenium Chrome driver used for browser automation.
            current_link (str): URL of the current page where pagination begins.
        
        Returns:
            None.
        """
        # random_sleep_time = [0.8, 0.6, 0.7, 0.5]
        cur_date = self.end_date
        # i = 0
        
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
                # i += 1
            
                cur_date = date    
                
            else: # 특정 범위의 날짜를 전부 크롤링 했다면
                logger.info(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                break
        return
    
    def get_html_of_post(self, driver, url:str):
        """
        Retrieves HTML content of a post and parses it with BeautifulSoup.
        
        Loads the specified URL using a Selenium WebDriver instance and applies a randomized delay.
        Retries up to MAX_TRY times if the page source cannot be parsed, logging an error on each failure.
        Returns a BeautifulSoup object containing the parsed HTML if successful, or False if all attempts fail.
        
        Args:
            driver: A Selenium WebDriver instance used to load the webpage.
            url (str): The URL of the post to retrieve.
        
        Returns:
            BeautifulSoup: Parsed HTML content if the retrieval is successful.
            bool: False if all retry attempts fail.
        """
        headers = {'User-Agent': "Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)"}
        for _ in range(self.MAX_TRY):
            # response = requests.get(url, headers=headers)
            
            driver.get(url)
            time.sleep(WAIT_TIME - (random.randrange(50, 100)/100))
            soup = BeautifulSoup(driver.page_source, "html.parser")            
            if soup:
                return soup
            
            else:# 페이지 접근 재시도
                logger.error(f"❌ {url} request FAILED!")
                time.sleep(self.RETRY_WAITS)
                continue
        return False
            
    def html_parser(self, driver:webdriver.Chrome, post_info:dict, parsed_post:BeautifulSoup):
        """
        Extracts post details, metadata, and comments from HTML content.
        
        Args:
            driver (webdriver.Chrome): Selenium Chrome driver used for dynamic webpage interactions.
            post_info (dict): Dictionary containing post metadata with keys such as 'url' and 'id'.
            parsed_post (BeautifulSoup): Parsed HTML object of the post page.
        
        Returns:
            dict: A dictionary with the extracted data including:
                - post_id (int): Unique identifier for the post.
                - post_url (str): URL of the post.
                - title (str): Title of the post.
                - content (str): Main text content of the post.
                - created_at (str): Post creation timestamp in ISO format (YYYY-MM-DDTHH:MM:SS).
                - view_count (int): Number of views.
                - upvote_count (int): Count of upvotes.
                - downvote_count (int): Count of downvotes.
                - comment_count (int): Total number of comments.
                - comments (list[dict]): List of comment dictionaries containing details such as comment_id, content, 
                  reply status, creation time, and vote counts.
        
        Example:
            >>> result = instance.html_parser(driver, {"url": "http://example.com/post/1", "id": 1}, soup)
            >>> print(result["title"])
        """
        print("Now Watching : " , driver.current_url)
        def parse_main_content(target_element):
            """
            Extracts post content along with upvote and downvote counts from an HTML element.
            
            Locates the container for the post content, retrieving the text with line breaks preserved.
            Also extracts the upvote count from the element with classes "up_num font_red" and
            the downvote count from the element with class "down_num", converting both to integers.
            
            Args:
                target_element (bs4.element.Tag): The BeautifulSoup element containing the post details.
            
            Returns:
                tuple: A tuple of three elements:
                    - content (str): The text content of the post with HTML line breaks converted to newlines.
                    - upvote (int): The number of upvotes.
                    - downvote (int): The number of downvotes.
            """
            write_div = target_element.find("div", class_="write_div")
            gaechu = int(target_element.find("p", class_="up_num font_red").get_text(strip=True))
            bichu = int(target_element.find("p", class_="down_num").get_text(strip=True))
            content = write_div.get_text(separator="\n", strip=True)  # <br>을 \n으로 변환, 공백 제거
            return content, gaechu, bichu

        def parse_comments(soup:BeautifulSoup):
            """
            댓글 및 대댓글 정보를 추출하여 리스트로 반환한다.
            
            HTML 파싱 결과 BeautifulSoup 객체로부터 댓글 및 대댓글을 찾아 각 항목을
            딕셔너리로 수집한다. 수집되는 딕셔너리는 다음 키를 포함한다:
                - comment_id (int): 댓글 또는 대댓글의 고유 식별자. 대댓글은 상위 댓글의 ID 사용.
                - content (str): 댓글 또는 대댓글의 텍스트 내용.
                - is_reply (int): 댓글은 0, 대댓글은 1.
                - created_at (str): ISO 8601 형식("yyyy-mm-ddThh:mm:ss")으로 변환된 작성 시간.
                - upvote_count (int): 추천 수, 기본값은 0.
                - downvote_count (int): 비추천 수, 기본값은 0.
            
            광고 댓글(클래스에 "dory" 포함)은 제외된다.
            
            Args:
                soup (BeautifulSoup): HTML이 파싱된 BeautifulSoup 객체.
            
            Returns:
                list[dict]: 댓글과 대댓글 정보를 포함한 딕셔너리 리스트.
            """
            comment_list = []
            comment_ul = soup.find("ul", class_="cmt_list")
            
            if not comment_ul:
                # print("no comments")
                return comment_list  # 댓글이 없으면 빈 리스트 반환
            
            for li in comment_ul.find_all("li", recursive=False):  # 최상위 li만 탐색 (대댓글 제외)
                # 🔹 댓글인지 대댓글인지 구분
                is_reply = 0  # 기본적으로 댓글(0)
                
                if "dory" in li.get("class", []): # 광고댓글 거르기 (댓글돌이 광고)
                    continue
                
                # 🔹 댓글 내용
                if (cmt_id := li.get('id')) and not li.select_one("p.del_reply"): # 댓글이면
                    content_tag = li.select_one("p.usertxt.ub-word")
                    content = content_tag.get_text(strip=True) if content_tag else ""

                    # 🔹 작성 시간 (datetime 변환)
                    created_at = li.select_one("span.date_time").get_text(strip=True)
                    # isoformat으로 변환
                    ymd, hms = md_to_ymd(created_at).split()
                    created_at = 'T'.join([convert_date_format(ymd), hms])
                    
                    comment_id = int(cmt_id.split('_')[-1])
                    
                    # 🔹 리스트에 추가
                    comment_list.append({
                        "comment_id": comment_id,
                        "content": content,
                        "is_reply": is_reply,
                        "created_at": created_at,
                        "upvote_count": 0,
                        "downvote_count": 0
                    })
                else:
                    comment_id = None
                
                if li.find("div", class_="reply_box"):
                    is_reply = 1  # 대댓글(1)
                # 🔹 대댓글 탐색
                reply_ul = li.select_one("ul.reply_list")
                
                if reply_ul:
                    reply_parent_id = int(reply_ul.get('id').split('_')[-1])
                    for reply_li in reply_ul.find_all("li", class_="ub-content"):
                        # reply_parent_id = comment_id
                        if reply_content_tag := reply_li.select_one("p.usertxt.ub-word"):
                            reply_content = reply_content_tag.get_text(strip=True) if reply_content_tag else ""

                            reply_created_at = reply_li.select_one("span.date_time").get_text(strip=True)
                            ymd, hms = md_to_ymd(reply_created_at).split()
                            reply_created_at = 'T'.join([convert_date_format(ymd), hms])

                            comment_list.append({
                                "comment_id": reply_parent_id,
                                "content": reply_content,
                                "is_reply": 1,  # 대댓글
                                "created_at": reply_created_at,
                                "upvote_count": 0,
                                "downvote_count": 0                            
                            })
                        else: continue

            return comment_list

        def scrape_all_comment_pages(driver, soup):
            """
            Aggregates all comments across paginated comment pages.
            
            Extracts the total number of comments from the initial BeautifulSoup object and collects comments from the
            first page using a helper function. If pagination is found, iterates over each numeric page button by executing
            its JavaScript to load subsequent comment pages, waits for the new content to load, and accumulates the comments.
            
            Args:
                driver (webdriver.Chrome): Selenium WebDriver used to execute JavaScript and navigate between pages.
                soup (BeautifulSoup): Parsed HTML content of the initial comment page.
            
            Returns:
                tuple:
                    int: The total comment count as indicated on the page.
                    list: A list of comments collected from all paginated comment pages.
            """
            comment_count_tag = soup.find('span', class_='gall_comment')
            comment_count = int(comment_count_tag.find('a').text[len("댓글 "):]) if comment_count_tag else 0
            
            all_comments = []  # 모든 댓글을 저장할 리스트

            # 🔹 첫 번째 페이지 댓글 수집
            comments = parse_comments(soup)
            all_comments.extend(comments)
            

            # 🔹 다음 댓글 페이지 버튼 찾기
            paging_box = soup.select_one("div.cmt_paging")
            if not paging_box:
                # print("댓글 페이지네이션이 없음.")
                return comment_count, all_comments

            next_page_btns = paging_box.find_all("a", href=True)

            for btn in next_page_btns:
                page_number = btn.get_text(strip=True)
                if page_number.isdigit():
                    # print(f"이동 중: 댓글 페이지 {page_number}")

                    # 🔹 JavaScript 실행하여 댓글 페이지 이동
                    driver.execute_script(btn["href"])

                    # 🔹 새로운 페이지 HTML을 가져오기 위해 대기
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.cmt_paging"))
                    )

                    # 🔹 새로운 soup 업데이트 후 댓글 추가 수집
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    comments = parse_comments(soup)
                    all_comments.extend(comments)

            return comment_count, all_comments

        post_url = post_info['url']
        post_id = post_info['id']
        # type : yyyy.mm.dd HH:MM:SS
        created_at = parsed_post.find("span", class_="gall_date").get_text(strip=True)
        ymd, hms = created_at.split()
        created_at = 'T'.join([convert_date_format(ymd), hms])
        # created_at.replace('-', '.') --> 아니 이게 살아있었는데ㅔ 어떻게 23-08-07 이런 식으로 저장된거지?
        # created_at = datetime.strptime(created_at, "%Y.%m.%d %H:%M:%S")
        title = parsed_post.find("span", class_="title_subject").get_text(strip=True)
        view_count = int(parsed_post.find("span", class_="gall_count").get_text(strip=True)[len("조회 "):])
        content, up_vote, down_vote = parse_main_content(parsed_post)
        comment_count, comment_list = scrape_all_comment_pages(driver, parsed_post)
        
        parsed_finally = {
            "post_id" : post_id ,
            "post_url" : post_url,
            "title" : title,
            "content" : content,
            "created_at" : created_at,
            "view_count" : view_count,
            "upvote_count" : up_vote,
            "downvote_count" : down_vote,
            "comment_count" : comment_count,
            "comments" : comment_list
        }
                
        return parsed_finally
    
    def save_json(self, parsed_json:json, post_info:dict):
        """
        Saves parsed JSON data to a file in a structured directory based on car ID and post date.
        
        Args:
            parsed_json (json): The JSON data to be saved.
            post_info (dict): Metadata dictionary that must include:
                - 'date': A date string where the substring from index 5 is used to form the directory.
                - 'id': A unique identifier used as the JSON file name.
        
        The file path is formatted as:
          "extracted/{car_id}/{file_date}/raw/dcinside/{id}.json",
        where {file_date} is derived from the 'date' string in post_info.
        
        Directories are created if they do not exist. Any exceptions during the file write
        are caught and logged; no exception is propagated.
        """
        file_date = post_info['date'][5:]
        file_path = f"extracted/{self.car_id}/{file_date}/raw/dcinside/{post_info['id']}.json"
        directory = os.path.dirname(file_path)

        
        if not os.path.exists(directory):  # 디렉토리가 존재하지 않으면
            os.makedirs(directory)  # 디렉토리 생성
        
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                # file.write(html_source)
                json.dump(parsed_json, file, ensure_ascii=False, indent=4)
            # print(f"HTML 소스가 {file_path}에 저장되었습니다.")
            
        except Exception as e:
            logger.error(f"❌ 파일 저장 중 오류 발생: {e}")   
        
    def run_crawl(self,):
        # 드라이버 세팅
        """
        Execute the complete crawl sequence.
        
        Initializes the Selenium driver and navigates to the search entry URL within the specified date range.
        Collects valid post links by traversing the pagination, retrieves each post's HTML content, parses the
        required details, and saves the extracted data in JSON format. A randomized delay between posts is used
        to emulate natural browsing behavior.
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
            # print(post['url'])
            # print(f"Progressing... [{i+1} / {len(self.post_link)}]")
            
            # random_sleep_time = [0.8, 0.6, 0.7, 0.5]
            parsed_source = self.get_html_of_post(driver, post['url'])
            res_json = self.html_parser(driver, post, parsed_source)
            
            logger.info(f"💿 ⏎ Saving...[{i+1} / {len(self.post_link)}]")
            self.save_json(res_json, post)
                
            time.sleep(1 + random.randrange(50, 100) / 100)
                    

    
if __name__=="__main__":
    
    car = {'산타페': [ # 차종
                '산타페', # 해당 차종의 이명
                '싼타페']
        }   
  
    s_date="2024-12-30"
    e_date="2025-01-02"
    
    daily_batch = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    
    # car_keyword는 lambda_handler에서 event로 처리하게 할 것
    crawler = DC_crawler(s_date, e_date, car_id="casper", car_keyword="캐스퍼", is_daily_batch=True)
    
    logger.info("Running crawler")
    crawler.run_crawl()
    
    logger.info("✅ Crawling Finished")
    