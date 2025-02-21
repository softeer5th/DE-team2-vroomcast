import json
import os

from post_extractor import (
    _get_soup,
    _parse_comments,
    _parse_post_comment_count,
    _parse_post_content,
    _parse_post_created_at,
    _parse_post_title,
    _parse_post_upvote_count,
    _parse_post_view_count,
    extract_post,
)
from post_info_list_extractor import (
    _fetch_search_result,
    _find_start_page,
    get_post_infos,
)


def test_post_extractor():
    """
    Run a parsing test on a sample post HTML file.
    
    Opens "sample/post.html" using UTF-8 encoding, parses its content to generate a BeautifulSoup
    object via _get_soup, and prints the extracted post details including title, creation date,
    view count, upvote count, comment count, content, and comments using corresponding helper functions.
    
    Raises:
        FileNotFoundError: If the sample HTML file does not exist.
    """
    with open("sample/post.html", "r", encoding="utf-8") as f:
        html = f.read()

    soup = _get_soup(html)

    print("=== 보배드림 게시글 파싱 테스트 ===")
    print(f"제목: {_parse_post_title(soup)}")
    print(f"작성 날짜: {_parse_post_created_at(soup)}")
    print(f"조회수: {_parse_post_view_count(soup)}")
    print(f"추천수: {_parse_post_upvote_count(soup)}")
    print(f"댓글수: {_parse_post_comment_count(soup)}")
    print("\n=== 본문 내용 ===")
    print(_parse_post_content(soup))
    print("\n=== 댓글 목록 ===")
    print(_parse_comments(soup))


# pytest -v -s test.py::test_get_post_infos
def test_get_post_infos():
    keyword = "아반떼"
    start_datetime = "2023-06-01T12:00:00"
    end_datetime = "2023-07-01T12:00:00"

    post_infos = get_post_infos(keyword, start_datetime, end_datetime)

    print("=== 검색 결과 테스트 ===")
    print("Post 개수:", len(post_infos))
    print(post_infos)


# pytest -v -s test.py::test_find_start_page
def test_find_start_page():
    """
    Tests the determination of the starting page for search results and validates post extraction.
    
    Using the preset keyword "아반떼" with the date "2023-07-01" as both the starting and ending date, the test:
    - Retrieves the starting page number via _find_start_page.
    - Fetches the HTML search result of that page using _fetch_search_result.
    - Extracts post entries from the HTML with _get_soup and a CSS selector.
    - Prints the starting page, the count of posts, and the post list for manual verification.
    """
    keyword = "아반떼"
    start_date = "2023-07-01"
    end_date = "2023-07-01"

    start_page = _find_start_page(keyword, start_date, end_date)
    html = _fetch_search_result(keyword, start_page, start_date)

    post_list = _get_soup(html).select(".search_Community ul li")

    print("=== 시작 페이지 테스트 ===")
    print("Start page:", start_page)
    print("Post 개수:", len(post_list))
    print(post_list)


# pytest -v -s test.py::test_extract_post
def test_extract_post():
    """
    Test extraction of a specific post and save its parsed data.
    
    Extracts post details from a preset URL and post ID using the extract_post function.
    Prints the extracted data to the console and writes the result to 'sample/parsed_post.json'
    in JSON format with UTF-8 encoding. This test verifies that post extraction and JSON
    serialization function as expected.
    """
    url = "https://www.bobaedream.co.kr/view?code=national&No=2349037"
    id = "2349037"

    post = extract_post(url, id)

    print("=== 게시글 추출 테스트 ===")
    print(post)
    with open("sample/parsed_post.json", "w", encoding="utf-8") as f:
        json.dump(post, f, ensure_ascii=False, indent=2)


# pytest -v -s test.py::test
def test():
    """
    Test complete post extraction workflow.
    
    Retrieves post metadata for the keyword "아반떼" from 2025-02-15T12:00:00 to 2025-02-17T15:15:00 using get_post_infos(). Iterates through the returned posts, and for each one, calls extract_post() to obtain detailed post data. If extraction is successful, prints the post ID and creation date, then saves the post details as a JSON file in the "sample" directory (created if necessary).
    
    Returns:
        None
    """
    keyword = "아반떼"
    start_datetime = "2025-02-15T12:00:00"
    end_datetime = "2025-02-17T15:15:00"

    post_infos = get_post_infos(keyword, start_datetime, end_datetime)

    os.makedirs("sample", exist_ok=True)

    for post_info in post_infos:
        post = extract_post(post_info["url"], str(post_info["id"]), start_datetime, end_datetime)
        if not post:
            continue
        print(f"Post ID and Date {post['post_id']}, {post['created_at']}")
        with open(f"sample/{post_info['id']}.json", "w", encoding="utf-8") as f:
            json.dump(post, f, ensure_ascii=False, indent=2)


"""
브라우저에서 검색 테스트

fetch("https://www.bobaedream.co.kr/search", {
  method: "POST",
  headers: {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
  },
  body: new URLSearchParams({
    colle: "community",
    searchField: "ALL",
    page: "100",
    sort: "DATE",
    startDate: "2020-01-01",
    keyword: "싼타페"
  })
})
.then(response => response.text())
.then(html => {
  // 응답된 HTML을 새 창에서 보기
  const newWindow = window.open();
  newWindow.document.write(html);
});
"""
