from collections import OrderedDict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# 아주 큰 숫자
_INF = 999999999


def _get_soup(html: str) -> BeautifulSoup:
    """
    BeautifulSoup 객체 생성
    Args:
        html (str): HTML 문자열
    Returns:
        BeautifulSoup: BeautifulSoup 객체
    """
    return BeautifulSoup(html, "html.parser")


# 검색 결과 캐싱
_search_result_cache: OrderedDict[tuple[str, int], str] = OrderedDict()


def _cache_search_result(keyword: str, page: int, html: str) -> None:
    """
    검색 결과 캐싱 (최대 20개)
    Args:
        keyword (str): 검색 키워드
        page (int): 페이지 번호
        html (str): 검색 결과 HTML
    """
    if len(_search_result_cache) >= 20:
        # 캐시가 가득 차면 가장 오래된 데이터 삭제
        _search_result_cache.popitem(last=False)  # FIFO
    _search_result_cache[(keyword, page)] = html


def _fetch_search_result(keyword: str, page: int, start_date: str) -> str:
    """
    검색 결과 가져오기
    Args:
        keyword (str): 검색 키워드
        page (int): 페이지 번호
        start_date (str): 검색 시작 날짜
    Returns:
        str: 검색 결과 HTML
    """

    # 캐시 확인
    if (keyword, page) in _search_result_cache:
        return _search_result_cache[(keyword, page)]

    # 검색 URL
    url = "https://www.bobaedream.co.kr/search"

    # 검색 파라미터
    params = {
        "colle": "community",
        "searchField": "ALL",
        "page": page,
        "sort": "DATE",
        "startDate": start_date,
        "keyword": keyword,
    }

    # 헤더
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.bobaedream.co.kr",
        "Referer": "https://www.bobaedream.co.kr/search",
    }

    # POST 요청
    response = requests.post(url, data=params, headers=headers)
    response.raise_for_status()
    response.encoding = "utf-8"

    # 텍스트 추출 및 캐싱
    html = response.text
    _cache_search_result(keyword, page, html)

    return html


def _parse_post_date(post_tag: Tag) -> str:
    """
    게시글 날짜 추출
    Args:
        post_tag (Tag): 게시글 태그
    Returns:
        str: 게시글 날짜
    """
    post_date_raw = post_tag.select_one(".path span:last-child").text
    return "20" + "".join(post_date_raw.split()).replace(".", "-")


def _parse_post_url(post_tag: Tag) -> str:
    """
    게시글 URL 추출
    Args:
        post_tag (Tag): 게시글 태그
    Returns:
        str: 게시글 URL
    """
    post_title_elem = post_tag.select_one("dt a")
    return urljoin("https://www.bobaedream.co.kr", post_title_elem.get("href", ""))


def _parse_post_id(post_url: str) -> int:
    """
    게시글 ID 추출
    Args:
        post_url (str): 게시글 URL
    Returns:
        int: 게시글 ID
    """
    return int(post_url.split("No=")[1].split("&")[0])


def _parse_post_info(post_tag: Tag) -> dict:
    """
    게시글 목록 상의 게시글 정보 추출
    Args:
        post_tag (Tag): 게시글 태그
    Returns:
        dict: 게시글 정보
    """
    url = _parse_post_url(post_tag)
    return {
        "id": _parse_post_id(url),
        "url": url,
        "date": _parse_post_date(post_tag),
    }


def _find_start_page(
    keyword: str, start_date: str, end_date: str, chunk_size: int = 500
) -> int:
    """
    시작 페이지 찾기
    Args:
        keyword (str): 검색 키워드
        start_date (str): 검색 시작 날짜
        end_date (str): 검색 종료 날짜
        chunk_size (int): 페이지 단위
    Returns:
        int: 시작 페이지
    """

    def _get_community_section(page: int) -> Tag | None:
        """
        커뮤니티 섹션 추출
        Args:
            page (int): 페이지 번호
        Returns:
            Tag: 커뮤니티 섹션
        """
        html = _fetch_search_result(keyword, page, start_date)
        soup = _get_soup(html)
        community_section = soup.select_one(".search_Community")
        return community_section

    def _binary_search(start: int, end: int) -> int:
        """
        이분 탐색
        Args:
            start (int): 시작 페이지
            end (int): 종료 페이지
        Returns:
            int: 시작 페이지
        """

        # 이분 탐색 종료 조건
        if start > end:
            return start

        # 확인할 페이지
        mid = (start + end) // 2

        # 커뮤니티 섹션 추출에 실패한 경우 (페이지가 깨진 경우)
        if not (community_section := _get_community_section(mid)):

            # 좌우 이웃 페이지들을 확인하며 정상 페이지를 찾음
            left = mid - 1
            while left > 0 and not (community_section := _get_community_section(left)):
                left -= 1
            right = mid + 1
            # 오른쪽 이웃 페이지의 커뮤니티 섹션은 저장할 필요가 없음 (어차피 하나만 필요하기 때문)
            while not (_get_community_section(right)):
                right += 1
            # 왼쪽 이웃 페이지가 유효하지 않는 범위까지 이동한 경우
            if left == 0:
                left = right
        # 커뮤니티 섹션 추출에 성공한 경우
        else:
            left = mid
            right = mid

        # 게시글 목록 추출
        post_list = community_section.select("ul li")

        # 게시글 목록이 없는 경우 좌측 탐색 (start_date 이상의 페이지를 찾아야 함)
        if not post_list:
            return _binary_search(start, left - 1)

        # 게시글 목록의 가장 오래된 날짜 확인
        page_date = _parse_post_date(post_list[-1])

        # 해당 게시글의 가장 오래된 날짜가 종료 날짜보다 같거나 오래된 경우 좌측 탐색
        if page_date <= end_date:
            return _binary_search(start, left - 1)

        # 해당 게시글의 가장 오래된 날짜가 종료 날짜보다 빠른 경우 우측 탐색
        return _binary_search(right + 1, end)

    # 청크 단위 범위로 이분 탐색
    chunk_limit = _INF

    # 이분 탐색 수행
    for chunk in range(1, chunk_limit):
        start_chunk = (chunk - 1) * chunk_size + 1
        end_chunk = chunk * chunk_size
        result = _binary_search(start_chunk, end_chunk)
        # 결과가 해당 청크 범위 내에 있으면 반환
        if result <= end_chunk:
            return result

    # 못 찾으면 없는 페이지 반환
    return chunk_size * chunk_limit


def _parse_post_infos_per_page(
    soup: BeautifulSoup, start_date: str, end_date: str
) -> list[tuple[str, str]] | None:
    """
    페이지 별 게시글 목록 상의 게시글 정보 추출
    Args:
        soup (BeautifulSoup): BeautifulSoup 객체
        start_date (str): 검색 시작 날짜
        end_date (str): 검색 종료 날짜
    Returns:
        list: 게시글 정보 목록
    """
    community_section = soup.select_one(".search_Community")
    if not community_section:
        return None

    post_list = soup.select(".search_Community ul li")
    post_infos = []

    for post_tag in post_list:
        post_info = _parse_post_info(post_tag)
        if not post_info:
            continue
        if start_date <= post_info["date"] <= end_date:
            post_infos.append(post_info)

    return post_infos


def get_post_infos(keyword: str, start_datetime: str, end_datetime: str) -> list[dict]:
    """
    게시글 정보 가져오기
    Args:
        keyword (str): 검색 키워드
        start_datetime (str): 검색 시작 날짜
        end_datetime (str): 검색 종료 날짜
    Returns:
        list: 게시글 정보 목록
    """

    # 게시글 정보 목록
    post_infos = []

    # 시작 날짜와 종료 날짜
    start_date = start_datetime.split("T")[0]
    end_date = end_datetime.split("T")[0]

    # 시작 페이지 찾기
    start_page = _find_start_page(keyword, start_date, end_date)

    # 페이지 별 게시글 정보 추출
    for page in range(start_page, _INF):
        html = _fetch_search_result(keyword, page, start_date)
        soup = _get_soup(html)
        post_infos_per_page = _parse_post_infos_per_page(soup, start_date, end_date)
        if post_infos_per_page is None:
            continue
        if len(post_infos_per_page) == 0:
            break
        post_infos.extend(post_infos_per_page)

    # 게시글 정보 목록 반환
    return post_infos
