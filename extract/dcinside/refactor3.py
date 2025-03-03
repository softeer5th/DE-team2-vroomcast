# n시간 단위 배치를 상정한 코드입니다.


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta
from dateutil import parser
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time, json, logging, requests, os
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
    yyyy-mm-dd HH:MM:SS -> yyyy-mm-ddTHH:MM:SS (ISO Format)
    """
    return 'T'.join(date_str.split())

def md_to_ymd(date_str:str):
    """
    댓글 타임스탬프의 두 가지 날짜 형식을 입력받아 "yyyy-mm-dd HH:MM:SS" 형식으로 변환합니다.
    본문 및 댓글의 날짜 형식에 대응합니다.
    
    Args:
        date_str: 변환할 날짜 문자열 ("yyyy.mm.dd HH:MM:SS" 또는 "mm.dd HH:MM:SS" 형식)

    Returns:
        "yyyy-mm-dd HH:MM:SS" 형식으로 변환된 날짜 문자열
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
    입력된 시간 문자열이 이번 배치 시간과 3일 전 사이에 있는지 판단하는 함수.

    Args:
        time_str: "%Y-%m-%d %H:%M:%S" 형식의 시간 문자열.

    Returns:
        True: 입력된 시간이 현재 시간과 현재 시간의 6시간 전 사이에 있는 경우.
        False: 입력된 시간이 현재 시간과 현재 시간의 6시간 전 사이에 없는 경우.
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

class DC_crawler:
    MAX_TRY = 2
    RETRY_WAITS = 2
    post_link = [
    ]
    def __init__(self, s_date, e_date, car_id, car_keyword, is_daily_batch, batch, folder_date):
        self.start_date = s_date
        self.end_date = e_date
        self.car_id = car_id
        self.keyword = car_keyword
        self.search_url = [SEARCH_URL_TITLE + kw for kw in car_keyword]# SEARCH_URL_TITLE + car_keyword
        self.batch = batch
        self.folder_date = folder_date
        self.id_check = []
    # Chrome WebDriver 선언, Lambda 적용 시 주석 필히 보고 해제할 것!!!!!
    def _get_driver(self,):
        # 이 path는 로컬 실행 시 주석처리 하세요.
        # chrome_path = "/opt/chrome/chrome-headless-shell-mac-arm64"
        # driver_path = "/opt/chromedriver"   

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
        현재 페이지에서 게시글들의 링크를 수집합니다.
        """
        posts = soup.select("tr.ub-content.us-post")
        
        for post in posts:
            # 날짜 검증
            date = post.select_one("td.gall_date")['title'] if post.select_one("td.gall_date") else "0000-00-00 00:00:00"
            # print(date)
            time_checker = is_time_in_range(date, self.start_date, self.end_date)
            if time_checker == "UNDER":
                logger.info(f"❗️ Stopped by found date {str(date)}")
                return False
            elif time_checker == "OVER":
                logger.info(f"❗️ This post Over end_date : {str(date)}")
                continue
            
            ymd_date = str(date).split()[0] # y-m-d
            
            # 날짜 넘어갈 시 로그 작성
            if ymd_date != cur_date:
                logger.info(f"Collecting 🔗 of {ymd_date}")
                cur_date = ymd_date
              
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
        페이징 박스를 순회합니다.
        시간 역순으로 순회합니다. 
        (페이징 박스는 정방향 순회, 보이는 게시글은 시간 역순)
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
                
                time.sleep(random.randrange(500, 1000) / 1000)
                cur_date = date    
                
            else: # 특정 범위의 날짜를 전부 크롤링 했다면
                logger.info(f"✅ Crawling {self.start_date} ~ {self.end_date} Finished")
                break
        return
    
    def get_html_of_post(self, driver, url:str):
        """
        각 게시글의 html source를 가져옵니다.
        가져온 source를 반환합니다.
        """
        # headers = {'User-Agent': "Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)"}
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
        print("Now Watching ▶ " , driver.current_url)
        def parse_main_content(target_element):
            """
            게시글 본문 크롤링
            Returns:
                본문 내용, 추천 수, 비추 수
            """
            write_div = target_element.find("div", class_="write_div")
            gaechu = int(target_element.find("p", class_="up_num font_red").get_text(strip=True))
            bichu = int(target_element.find("p", class_="down_num").get_text(strip=True))
            content = write_div.get_text(separator="\n", strip=True)  # <br>을 \n으로 변환, 공백 제거
            return content, gaechu, bichu

        def parse_comments(soup:BeautifulSoup):
            """
            댓글 및 대댓글을 수집하여 리스트로 반환하는 함수.
            
            Args:
                soup (BeautifulSoup): BeautifulSoup으로 파싱된 HTML
            
            Returns:
                list[dict]: 댓글과 대댓글을 포함한 리스트
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

        def scrape_all_comment_pages(driver, soup):
            """
            주어진 soup을 기반으로 댓글 페이지를 순회하며 모든 댓글을 수집하는 함수.
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
        file_path = f"extracted/{self.car_id}/{self.folder_date}/{self.batch}/raw/dcinside/{post_info['id']}.json"
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
        driver=self._get_driver()
        logger.info("✅ Driver Successfully Set.")
        
        for url in self.search_url:
            print(f"URL with Search Keyword: {url}")
            # 검색 기간 내 가장 최신 게시글 검색 결과 접근
            end_point = self.get_entry_point(driver, url=url)
            logger.info("✅ Successfully accessed to init date")
            
            # 접근 위치로부터 거슬러 올라가며 게시글 링크 수집
            self.page_traveler(driver, end_point)
        
        # 수집된 링크를 방문하며 html 소스 저장
        fail_count = 0
        for i, post in enumerate(self.post_link):

            parsed_source = self.get_html_of_post(driver, post['url'])
            res_json = self.html_parser(driver, post, parsed_source)
            
            logger.info(f"💿 ⏎ Saving...[{i+1} / {len(self.post_link)}]")
            try:
                self.save_json(res_json, post)
            except:
                fail_count += 1
                continue
                
            time.sleep(random.randrange(0, 50) / 100)
        return fail_count, len(self.post_link)
                    

    
if __name__=="__main__":
    init_time = time.time()
    
    airflow_json = {
	"bucket": "vroomcast-s3",
	"keywords": ["ev3"],
	"car_id": "ev3",
	"date": "2025-02-20",
	"batch": 0,
	"start_datetime": "2024-05-07T00:00:00",
	"end_datetime": "2024-05-30T00:00:00"
    }
    
    s_date = ' '.join(airflow_json['start_datetime'].split('T'))
    e_date = ' '.join(airflow_json['end_datetime'].split('T'))
    
    
    logger.info(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    
    # car_keyword는 lambda_handler에서 event로 처리하게 할 것
    crawler = DC_crawler(s_date, e_date, car_id=airflow_json['car_id'], car_keyword=airflow_json['keywords'], is_daily_batch=True, batch = airflow_json['batch'], folder_date=airflow_json['date'])
    
    logger.info("Running crawler")
    try:
        failed, tried = crawler.run_crawl()    
        logger.info("✅ Crawling Finished")
            
        finished_time = time.time()
        delta = finished_time - init_time
        
        return_params = {
            "statusCode": 200,
            "body": {
                "success": True,
                "end_time": convert_date_format(datetime.now().strftime("%y-%m-%d %H:%M:%S")),
                "duration": delta,
                "car_id": airflow_json['car_id'],
                "date": airflow_json['date'],
                "batch": airflow_json['batch'],
                "start_datetime": airflow_json['start_datetime'],
                "end_datetime": airflow_json['start_datetime'],
                "attempted_posts_count": tried,
                "extracted_posts_count": tried - failed
                }
        }
        pprint.pprint(return_params)
    
    except:
        finished_time = time.time()
        delta = finished_time - init_time        
        return_params = {
            "statusCode": 500,
            "body": {
                "success": False,
                "end_time": convert_date_format(datetime.now().strftime("%y-%m-%d %H:%M:%S")),
                "duration": delta,
                "car_id": airflow_json['car_id'],
                "date": airflow_json['date'],
                "batch": airflow_json['batch'],
                "start_datetime": airflow_json['start_datetime'],
                "end_datetime": airflow_json['start_datetime'],
                "attempted_posts_count": tried,
                "extracted_posts_count": tried - failed
                }
    }    