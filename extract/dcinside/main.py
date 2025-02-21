from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
# from tempfile import mkdtemp
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta
from dateutil import parser
# from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time, json, logging, os
from bs4 import BeautifulSoup
import boto3, random, pprint

logging.basicConfig(level=logging.INFO)  # 로그 레벨 설정
logger = logging.getLogger(__name__)


BASE_URL = "https://gall.dcinside.com/board/lists/?id=car_new1"
WAIT_TIME = 2

# 제목만 / 제목+내용
SEARCH_URL_TITLE = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject&s_keyword="
SEARCH_URL_TITLE_AND_CONTENT = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject_memo&s_keyword="  
    
def convert_date_format(date_str:str):
    """
    Converts a datetime string to ISO 8601 format by replacing the space separator with "T".
    
    Args:
        date_str (str): A date string in the format "yyyy-mm-dd HH:MM:SS".
    
    Returns:
        str: The date string in ISO 8601 format ("yyyy-mm-ddTHH:MM:SS").
    
    Example:
        >>> convert_date_format("2024-03-15 12:00:00")
        '2024-03-15T12:00:00'
    """
    return 'T'.join(date_str.split())

def md_to_ymd(date_str:str):
    """
    두 가지 날짜 형식 문자열을 'yyyy-mm-dd HH:MM:SS' 형식으로 변환합니다.
    
    입력 날짜 문자열은 "yyyy.mm.dd HH:MM:SS" 또는 "mm.dd HH:MM:SS" 형식이어야 하며,
    후자의 경우 현재 연도를 추가하여 변환됩니다. 입력 형식이 두 경우와 일치하지 않으면
    "Invalid date format" 문자열을 반환합니다.
    
    Args:
        date_str (str): 변환할 날짜 문자열. 지원 형식은 "yyyy.mm.dd HH:MM:SS" 또는 "mm.dd HH:MM:SS"입니다.
    
    Returns:
        str: "yyyy-mm-dd HH:MM:SS" 형식으로 변환된 날짜 문자열 또는 형식이 올바르지 않을 경우
             "Invalid date format" 문자열.
    """
    try:
        # "yyyy.mm.dd HH:MM:SS" 형식인 경우 그대로 반환
        date_obj = datetime.strptime(date_str, "%Y.%m.%d %H:%M:%S")
        return date_obj.strftime("%Y-%m-%d %H:%M:%S")
    
    except ValueError:
        try:
            # "mm.dd HH:MM:SS" 형식인 경우 "2025.mm.dd HH:MM:SS"로 변환 후 적용
            date_obj = datetime.strptime(date_str, "%m.%d %H:%M:%S")
            return date_obj.replace(year=datetime.now().year).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return "Invalid date format"

def is_time_in_range(time_str, start_time, end_time):
    """
    Determine the relation of a target time to a given time range.
    
    Parses the target time as well as the start and end boundaries from strings into datetime
    objects. Returns "IN" if the target time is within the range, "OVER" if it is later than the
    end, and "UNDER" if it is earlier than the start. If the target time is not in the expected
    "%Y-%m-%d %H:%M:%S" format, returns False.
    
    Args:
        time_str (str): Target time in "%Y-%m-%d %H:%M:%S" format.
        start_time (str): Start boundary of the range in "%Y-%m-%d %H:%M:%S" format.
        end_time (str): End boundary of the range in "%Y-%m-%d %H:%M:%S" format.
    
    Returns:
        str or bool: "IN" if time_str is between start_time and end_time,
        "OVER" if time_str is later than end_time,
        "UNDER" if time_str is earlier than start_time,
        or False if time_str is in an invalid format.
    
    Raises:
        ValueError: If start_time or end_time are not in the expected "%Y-%m-%d %H:%M:%S" format.
    """

    try:
        input_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False  # 잘못된 형식의 문자열

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    if start_time <= input_time <= end_time:
        return "IN"
    elif input_time > end_time: 
        return "OVER"
    else: return "UNDER"

    # return start_time <= input_time <= end_time  

def get_driver():
    """
    Return a configured headless Chrome WebDriver instance.
    
    Configures Chrome options for headless execution with resource optimizations,
    sets the binary location for Chrome, and initializes the WebDriver service using
    the specified chromedriver executable. Intended for web scraping in environments
    without a graphical interface.
    
    Returns:
        selenium.webdriver.Chrome: A headless Chrome WebDriver instance.
    
    Example:
        >>> driver = get_driver()
        >>> driver.get("https://example.com")
    """
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    # chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    # chrome_options.add_argument(f"--data-path={mkdtemp()}")
    # chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    # chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    # chrome_options.add_argument("--log-path=/tmp")
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    # prefs = {
    #     "profile.managed_default_content_settings.images": 2,  # 이미지 비활성화
    #     "profile.managed_default_content_settings.ads": 2,     # 광고 비활성화
    #     "profile.managed_default_content_settings.media": 2    # 비디오, 오디오 비활성화
    # }
    # chrome_options.add_experimental_option("prefs", prefs)

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        # service_log_path="/tmp/chromedriver.log"
    )
    driver = Chrome(
        service=service, # 도커 환경에서 사용시 주석 해제하세요.
        options=chrome_options
    )

    return driver
    # if driver:
    #     print("✅ Driver Successfully Set.")
    #     return driver
    # else:
    #     print("❌ Driver Setting Failed.")
    #     return False    
    
class DC_crawler:
    MAX_TRY = 2
    RETRY_WAITS = 2
    post_link = [
    ]
    
    def __init__(self, s_date, e_date, car_id, car_keyword, bucket_name, batch, folder_date):
        """
        Initializes a new crawler instance with configuration parameters for date range, car filters, and S3 storage.
        
        Args:
            s_date (str): Start date for the crawling period.
            e_date (str): End date for the crawling period.
            car_id (str): Identifier for the car board to target.
            car_keyword (Iterable[str]): Keywords used to construct search URLs.
            bucket_name (str): S3 bucket name for storing crawled data.
            batch (str): Batch identifier for grouping crawled posts.
            folder_date (str): Date string used to organize storage folders.
        
        Attributes:
            start_date (str): The crawling period's start date.
            end_date (str): The crawling period's end date.
            car_id (str): The target car board identifier.
            keyword (Iterable[str]): List of keywords used for the search.
            search_url (List[str]): List of search URLs constructed by appending each keyword to the base URL.
            BUCKET_NAME (str): The target S3 bucket name.
            folder_date (str): The folder date for organizing stored JSON files.
            batch (str): Identifier for the current crawl batch.
            id_check (list): List to track processed post IDs and avoid duplicates.
            s3 (boto3.client): Initialized S3 client for file upload operations.
        """
        self.start_date = s_date
        self.end_date = e_date
        self.car_id = car_id
        self.keyword = car_keyword
        self.search_url = [SEARCH_URL_TITLE + kw for kw in car_keyword]
        self.BUCKET_NAME = bucket_name
        self.folder_date = folder_date
        self.batch = batch
        self.id_check = []
        self.s3 = boto3.client("s3")
        
    # Chrome WebDriver 선언, Lambda 적용 시 주석 필히 보고 해제할 것!!!!!

    
    def get_entry_point(self, driver:webdriver.Chrome, url):
        """
        Navigates to the specified URL and triggers a date-based search.
        
        Uses the provided Selenium WebDriver to load the page, opens the date picker,
        enters the target search date (derived from the crawler's end_date attribute), and
        clicks the search button. Returns the URL of the search results page.
        
        Args:
            driver (webdriver.Chrome): The Selenium WebDriver used to interact with the web page.
            url (str): The URL to open and initiate the date-based search.
        
        Returns:
            str: The URL of the page showing the search results after applying the date filter.
        
        Raises:
            TimeoutException: If any required web element (date picker, input field, or search button)
                fails to become clickable or present within the allotted time.
        """
        s_date = self.start_date
        e_date = self.end_date.split()[0]
        
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
        Collects and processes post links and metadata from a parsed HTML page.
        
        Iterates over HTML elements representing posts, extracting each post’s date,
        ID, and URL. Dates are compared against the allowed range (defined by
        self.start_date and self.end_date): if a post’s date is before the start date,
        processing is terminated by returning False, and posts with dates after the end
        date are skipped. When a new day (YYYY-MM-DD) is encountered, a log entry is made.
        Valid posts are appended to self.post_link as a dictionary with keys "url",
        "id", and "date", while duplicates are avoided using self.id_check.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content of the page containing posts.
            cur_date (str): Current day marker in "YYYY-MM-DD" format to track date transitions.
        
        Returns:
            Union[str, bool]: The last processed date in "YYYY-MM-DD" format, or False if a
            post dated before the allowed start_date was encountered.
        """
        posts = soup.select("tr.ub-content.us-post")
        
        for post in posts:
            # 날짜 검증
            date = post.select_one("td.gall_date")['title'] if post.select_one("td.gall_date") else "0000-00-00 00:00:00"
            
            time_checker = is_time_in_range(date, self.start_date, self.end_date)
            if time_checker == "UNDER":
                logger.info(f"❗️ Stopped by found date {str(date)}")
                return False
            elif time_checker == "OVER":
                logger.info(f"❗️ This post Over end_date : {str(date)}")
                continue
            
            ymd_date = str(date).split()[0]
            
            # 날짜 넘어갈 시 로그 작성
            if ymd_date != cur_date:
                logger.info(f"Collecting 🔗 of {ymd_date}")
                print(f"Collecting 🔗 of {ymd_date}")
                cur_date = date
              
            gall_num = int(post.select_one("td.gall_num").get_text(strip=True))
            dc_url = "https://gall.dcinside.com"
            title_tag = post.select_one("td.gall_tit.ub-word a")
            link = dc_url + title_tag["href"] if title_tag else "링크 없음"
            
            if gall_num not in self.id_check:
                self.id_check.append(link)
                post_info = {
                    "url" : link,
                    "id" : gall_num,
                    "date" : date # y-m-d H:M:S
                }
            
                self.post_link.append(post_info)
            else:
                logger.info("This Link is Already Exists")
                continue
        return ymd_date
    
    def page_traveler(self, driver:webdriver.Chrome, current_link:str):
        """
        Traverse pagination pages, updating links until posts fall outside the specified date range.
        
        Loads each page using the Selenium WebDriver and extracts post dates using crawl_post_link.
        If a valid date is returned, locates the next page link via the pagination control, constructs
        its full URL, and updates the current date. Randomized sleep intervals simulate natural browsing
        behavior. Iteration continues until no valid date is found, at which point completion is logged.
        
        Args:
            driver (webdriver.Chrome): Selenium WebDriver instance for navigating web pages.
            current_link (str): URL of the current page to start pagination traversal.
        
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

                time.sleep(random.randrange(50, 100) / 100)            
                cur_date = date    
                
            else: # 특정 범위의 날짜를 전부 크롤링 했다면
                logger.info(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                print(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                break
        return
    
    def get_html_of_post(self, driver:webdriver.Chrome, url:str):
        """
        Retrieve and parse the HTML content of a post.
        
        Attempts to load the post at the given URL using a Selenium Chrome WebDriver instance and then parses its HTML
        using BeautifulSoup. Retries the operation up to self.MAX_TRY times with a randomized delay adjustment. Logs and
        prints error messages upon failure. Returns the parsed BeautifulSoup object on success, or False if all attempts fail.
        
        Args:
            driver (webdriver.Chrome): The Selenium Chrome WebDriver instance used for page navigation.
            url (str): The URL of the post to retrieve.
        
        Returns:
            BeautifulSoup: Parsed HTML content of the post if retrieval is successful.
            bool: False if the page retrieval fails after all retry attempts.
        """
        # headers = {'User-Agent': "Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)"}
        for _ in range(self.MAX_TRY):
            try:
                driver.get(url)
                time.sleep(WAIT_TIME - (random.randrange(50, 100) / 100))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                if soup:
                    return soup
            
            except:# 페이지 접근 재시도
                logger.error(f"❌ {url} request FAILED!")
                print(f"❌ {url} request FAILED!")
                time.sleep(self.RETRY_WAITS)
                continue
        return False
            
    def html_parser(self, driver:webdriver.Chrome, post_info:dict, parsed_post:BeautifulSoup):
        """
        Parses HTML content of a post page and extracts detailed post information along with associated comments.
        
        Prints the current URL and utilizes nested helpers to extract the main post content, vote counts, and comments. The process includes converting dates to ISO format and navigating through paginated comment sections via JavaScript execution. Extracted details comprise the post title, view count, content, creation date, upvote/downvote numbers, and a list of comments (including replies).
        
        Args:
            driver (webdriver.Chrome): Active Selenium WebDriver instance used for executing JavaScript and navigating pages.
            post_info (dict): Dictionary containing metadata of the post, including:
                - 'url' (str): URL of the post.
                - 'id': Unique identifier of the post.
                - 'date' (str): Original post creation date.
            parsed_post (BeautifulSoup): Parsed HTML of the post page.
        
        Returns:
            dict: Dictionary with the following keys:
                - "post_id": Unique identifier of the post.
                - "post_url" (str): URL of the post.
                - "title" (str): Post title.
                - "content" (str): Main text content of the post.
                - "created_at" (str): Post creation date in ISO format.
                - "view_count" (int): Number of views.
                - "upvote_count" (int): Count of upvotes.
                - "downvote_count" (int): Count of downvotes.
                - "comment_count" (int): Total number of comments.
                - "comments" (list[dict]): List of comment dictionaries, each containing:
                    - "comment_id": Identifier for the comment.
                    - "content" (str): Comment text.
                    - "is_reply" (int): Flag indicating if the comment is a reply (1) or a top-level comment (0).
                    - "created_at" (str): Comment creation date in ISO format.
                    - "upvote_count" (int): Upvote count (default 0).
                    - "downvote_count" (int): Downvote count (default 0).
        
        Example:
            parsed_data = html_parser(driver, post_info, parsed_post)
        """
        print("Now Parsing ▶ " , driver.current_url)

        def parse_main_content(target_element):
            """
            Extracts the post's main content and vote counts.
            
            Args:
                target_element (bs4.element.Tag): HTML element containing the post structure. It must include a
                    <div> with class "write_div" for the content, a <p> with class "up_num font_red" for the
                    recommendation count, and a <p> with class "down_num" for the disapproval count.
            
            Returns:
                tuple: A tuple containing:
                    - content (str): The extracted post content with <br> tags converted to newline characters.
                    - gaechu (int): The recommendation count.
                    - bichu (int): The disapproval count.
            
            Raises:
                AttributeError: If any required element is not found within target_element.
            """
            write_div = target_element.find("div", class_="write_div")
            gaechu = int(target_element.find("p", class_="up_num font_red").get_text(strip=True))
            bichu = int(target_element.find("p", class_="down_num").get_text(strip=True))
            content = write_div.get_text(separator="\n", strip=True)  # <br>을 \n으로 변환, 공백 제거
            return content, gaechu, bichu

        def parse_comments(soup:BeautifulSoup):
            """
            Extracts and returns a list of comments and replies from the parsed HTML.
            
            Iterates over the top-level list items in a <ul> element with class "cmt_list" to obtain
            comments (ignoring advertisement entries marked with the "dory" class and deleted entries).
            For each valid comment, extracts the text content, timestamp (converted to ISO format),
            and a unique comment ID. Additionally, any nested replies within a corresponding reply list
            are processed and appended with an indicator flag.
            
            Args:
                soup (BeautifulSoup): Parsed HTML containing the comment section.
            
            Returns:
                list[dict]: A list of dictionaries representing comments and replies. Each dictionary
                contains:
                    - comment_id (int): The unique identifier extracted from the element's id.
                    - content (str): The text content of the comment or reply.
                    - is_reply (int): 0 for top-level comments, 1 for replies.
                    - created_at (str): The timestamp converted to ISO format.
                    - upvote_count (int): Initialized to 0.
                    - downvote_count (int): Initialized to 0.
            """
            comment_list = []
            comment_ul = soup.find("ul", class_="cmt_list")
            
            if not comment_ul:
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
                    created_at = convert_date_format(md_to_ymd(created_at))
                    
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
                            
                            reply_created_at = convert_date_format(md_to_ymd(reply_created_at))

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

        def scrape_all_comment_pages(driver:webdriver.Chrome, soup:BeautifulSoup):
            """
            Aggregates comments across all paginated comment pages.
            
            Retrieves the initial comment count and first-page comments from the provided HTML,
            then iterates through additional comment pages by executing JavaScript links and
            waiting for new content to load. Comment entries are extracted using an external
            function `parse_comments`.
            
            Parameters:
                driver (webdriver.Chrome): Selenium Chrome WebDriver instance controlling the browser.
                soup (BeautifulSoup): Parsed HTML of the initial comment page containing comments and pagination.
            
            Returns:
                tuple: A tuple containing:
                    - int: The total count of comments as specified in the comment count tag.
                    - list: A list of all comment entries collected from the initial and subsequent pages.
            
            Example:
                >>> driver = get_driver()
                >>> driver.get("http://example.com/post")
                >>> soup = BeautifulSoup(driver.page_source, "html.parser")
                >>> total_comments, all_comments = scrape_all_comment_pages(driver, soup)
                >>> print("Total Comments:", total_comments)
                >>> for comment in all_comments:
                ...     print(comment)
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
                return comments, all_comments

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
        created_at = convert_date_format(post_info['date'])
        
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
        # post_date = str(md_to_ymd(post_info['date']))
        """
        Saves parsed post JSON data to an S3 bucket.
        
        Constructs an S3 file path using the crawler's attributes (car_id, folder_date, batch)
        and the post ID from post_info. Checks if the S3 directory (based on the file path)
        exists using head_object, then serializes the parsed JSON with indentation and uploads
        it to the specified S3 bucket as a JSON file.
        
        Args:
            parsed_json (json): Parsed post data to be saved in JSON format.
            post_info (dict): Dictionary containing post metadata; must include the key "id"
                              used for the filename.
        
        Notes:
            - Status messages are printed for folder verification, file creation, and upload results.
            - If the S3 directory is missing, a message is printed indicating directory creation,
              but no explicit directory creation is performed.
            - Exceptions during JSON serialization or S3 upload are caught and logged.
        """
        file_path = f"extracted/{self.car_id}/{self.folder_date}/{self.batch}/raw/dcinside/{post_info['id']}.json"
        directory = os.path.dirname(file_path)

        
        # if not os.path.exists(directory):  # 디렉토리가 존재하지 않으면
        #     os.makedirs(directory)  # 디렉토리 생성
        # 1. 폴더 존재 확인
        try:
            self.s3.head_object(Bucket=self.BUCKET_NAME, Key=directory)
            print("✅ S3 folder route exists")
        except:  # 폴더가 없는 경우
            print(f"❌ S3 folder route doesn't exists. Making directory...{directory}")
            
        try:
            # with open(file_path, "w", encoding="utf-8") as file:
                # file.write(html_source)
            web_data = json.dumps(parsed_json, ensure_ascii=False, indent=4)
            print(f"✅ Post ID: {post_info['id']} → File Created")
            
        except Exception as e:
            print(f"❌ json.dumps 중 오류 발생: {e}")       
            
        try:
            self.s3.put_object(
                Bucket = self.BUCKET_NAME,
                Key = file_path,
                Body = web_data,
                ContentType = "application/json"
            )     
            logger.info(f"✅ Successfully uploaded {post_info['id']}.json to s3-bucket")
            print(f"✅ Successfully uploaded {post_info['id']}.json to s3-bucket")

        except Exception as e:
            logger.error(f"❌ Error uploading file to S3: {e}", exc_info=True)
            print(f"❌ Error uploading file to S3: {e}", exc_info=True)

        
    def run_crawl(self,):
        # 드라이버 세팅
        
        """
        Runs the crawling process to collect, parse, and save post data.
        
        Initializes the Chrome WebDriver and iterates over each URL in `self.search_url` to:
        - Navigate to the entry point and set the date range.
        - Collect post links by traversing paginated results.
        - For each collected post link, retrieve the post's HTML, parse its content, and save the parsed data as JSON to an S3 bucket.
        
        The routine logs progress messages and applies a random delay between processing posts. If the WebDriver setup fails, an error message is printed and the process exits. Finally, the WebDriver is closed.
        
        Returns:
            tuple: A pair where the first element is the count of posts that failed to process, and the second element 
                   is the total number of collected post links.
        """
        try:
            driver = get_driver()
        except:
            print("🟥 Check Driver 🟥")
            exit(0)
        # if driver == False: 
        #     print("🟥 Check Driver 🟥")
        #     exit(0)
        for url in self.search_url:
            # 검색 기간 내 가장 최신 게시글 검색 결과 접근
            end_point = self.get_entry_point(driver, url=url)
            if end_point:
                logger.info("✅ Successfully accessed to init date")
                print("✅ Successfully accessed to init date")
            else:
                logger.warning(("❌ Failed to access init date"))
                print("❌ Failed to access init date")
                
            # 접근 위치로부터 거슬러 올라가며 게시글 링크 수집
            self.page_traveler(driver, end_point)
            print(f"✅ Gathering link completed : {len(self.post_link)} links")
        
        # 수집된 링크를 방문하며 html 소스 저장
        failed = 0
        for i, post in enumerate(self.post_link):

            try:
                parsed_source = self.get_html_of_post(driver, post['url'])
                res_json = self.html_parser(driver, post, parsed_source)
                
                logger.info(f"Saving...[{i+1} / {len(self.post_link)}]")
                print(f"Saving...[{i+1} / {len(self.post_link)}]")
                self.save_json(res_json, post)
            except:
                failed += 1
                continue
                
            time.sleep(random.randrange(0, 50) / 100)
        
        driver.close()
        return failed, len(self.post_link)  

def lambda_handler(event, context):
    """
    Lambda handler to initiate the DC_crawler web scraping process.
    
    Extracts configuration parameters from the event dictionary, sets up the crawler,
    and executes the crawling operation. Returns a response summarizing the crawling
    results including success status, duration, and counts of attempted and extracted posts.
    In case of an error during the crawling process, an error response is returned.
    
    Args:
        event (dict): Event data with the following keys:
            - "bucket" (str): S3 bucket name for storing the results.
            - "car_id" (str): Identifier for the target car forum board.
            - "keywords" (list of str): List of keywords for filtering posts.
            - "date" (str): Folder date in "YYYY-MM-DD" format.
            - "batch" (any): Identifier for the current batch.
            - "start_datetime" (str): Start date-time in ISO format (e.g., "YYYY-MM-DDTHH:MM:SS").
            - "end_datetime" (str): End date-time in ISO format (e.g., "YYYY-MM-DDTHH:MM:SS").
        context (object): AWS Lambda context object providing runtime information.
    
    Returns:
        dict: Response object containing:
            - "statusCode" (int): HTTP status code (200 for success, 500 for error).
            - "body" (dict): Dictionary with keys:
                - "success" (bool): Indicates if the crawl was successful.
                - "end_time" (str): Timestamp marking the end of the crawl, formatted via convert_date_format.
                - "duration" (float): Duration of the crawling process in seconds.
                - "car_id" (str): The car identifier.
                - "date" (str): The folder date.
                - "batch" (any): The batch identifier.
                - "start_datetime" (str): Start date-time of the crawl.
                - "end_datetime" (str): End date-time of the crawl.
                - "attempted_posts_count" (int): Number of posts the crawler attempted.
                - "extracted_posts_count" (int): Number of posts successfully extracted.
                - "Error" (Exception, optional): Exception details in case of an error.
    
    Example:
        >>> event = {
        ...     "bucket": "my-s3-bucket",
        ...     "car_id": "santafe",
        ...     "keywords": ["싼타페"],
        ...     "date": "2025-02-10",
        ...     "batch": "001",
        ...     "start_datetime": "2025-02-01T00:00:00",
        ...     "end_datetime": "2025-02-10T23:59:59"
        ... }
        >>> response = lambda_handler(event, None)
        >>> response["statusCode"]
        200
    """
    init_time = time.time()
    
    BUCKET_NAME = event.get('bucket')
    car_id      = event.get('car_id') # santafe
    car_keyword = event.get('keywords') # ["싼타페"]
    date        = event.get('date') # 2025-02-10
    batch       = event.get('batch')
    s_date      = event.get('start_datetime')
    e_date      = event.get('end_datetime')
        
    s_date = ' '.join(s_date.split('T'))
    e_date = ' '.join(e_date.split('T'))
    
    logger.info(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    print(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    # car_keyword는 lambda_handler에서 event로 처리하게 할 것
    crawler = DC_crawler(s_date, e_date, car_id=car_id, car_keyword=car_keyword, bucket_name=BUCKET_NAME, batch=batch, folder_date=date)
    
    print("▶ Running crawler...")
    logger.info("▶ Running crawler...")
    
    try:
        failed, tried = crawler.run_crawl()
        logger.info("✅ Crawling Finished")
        print("✅ Crawling Finished")
        finished_time = time.time()
        delta = finished_time - init_time
        
        return {
            "statusCode": 200,
            "body": {
                "success": True,
                "end_time": convert_date_format(datetime.now().strftime("%y-%m-%d %H:%M:%S")),
                "duration": delta,
                "car_id": car_id,
                "date": date,
                "batch": batch,
                "start_datetime": s_date,
                "end_datetime": e_date,
                "attempted_posts_count": tried,
                "extracted_posts_count": tried - failed                
                }
        }        
    except Exception as e:
        logger.info("❌ Crawling Not Finished With Errors")
        print("❌ Crawling Not Finished With Errors")
        finished_time = time.time()
        delta = finished_time - init_time
        return {
            "statusCode": 500,
            "body": {
                "success": False,
                "end_time": convert_date_format(datetime.now().strftime("%y-%m-%d %H:%M:%S")),
                "duration": delta,
                "car_id": car_id,
                "date": date,
                "batch": batch,
                "start_datetime": s_date,
                "end_datetime": e_date,
                "attempted_posts_count": tried,
                "extracted_posts_count": tried - failed,                
                "Error": e
                }
        }  
