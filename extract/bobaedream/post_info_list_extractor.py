from collections import OrderedDict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

_INF = 999999999


def _get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


_search_result_cache: OrderedDict[tuple[str, int], str] = OrderedDict()


def _cache_search_result(keyword: str, page: int, html: str) -> None:
    if len(_search_result_cache) >= 20:
        _search_result_cache.popitem(last=False)  # FIFO
    _search_result_cache[(keyword, page)] = html


def _fetch_search_result(keyword: str, page: int, start_date: str) -> str:
    if (keyword, page) in _search_result_cache:
        return _search_result_cache[(keyword, page)]

    url = "https://www.bobaedream.co.kr/search"

    params = {
        "colle": "community",
        "searchField": "ALL",
        "page": page,
        "sort": "DATE",
        "startDate": start_date,
        "keyword": keyword,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.bobaedream.co.kr",
        "Referer": "https://www.bobaedream.co.kr/search",
    }

    response = requests.post(url, data=params, headers=headers)
    response.raise_for_status()
    response.encoding = "utf-8"

    html = response.text
    _cache_search_result(keyword, page, html)

    return html


def _parse_post_date(post_tag: Tag) -> str:
    post_date_raw = post_tag.select_one(".path span:last-child").text
    return "20" + "".join(post_date_raw.split()).replace(".", "-")


def _parse_post_url(post_tag: Tag) -> str:
    post_title_elem = post_tag.select_one("dt a")
    return urljoin("https://www.bobaedream.co.kr", post_title_elem.get("href", ""))


def _parse_post_id(post_url: str) -> int:
    return int(post_url.split("No=")[1].split("&")[0])


def _parse_post_info(post_tag: Tag) -> dict:
    url = _parse_post_url(post_tag)
    return {
        "id": _parse_post_id(url),
        "url": url,
        "date": _parse_post_date(post_tag),
    }


def _find_start_page(
    keyword: str, start_date: str, end_date: str, chunk_size: int = 500
) -> int:
    def _get_community_section(page: int) -> Tag | None:
        html = _fetch_search_result(keyword, page, start_date)
        soup = _get_soup(html)
        community_section = soup.select_one(".search_Community")
        return community_section

    def _binary_search(start: int, end: int) -> int:
        if start > end:
            return start

        mid = (start + end) // 2

        if not (community_section := _get_community_section(mid)):
            left = mid - 1
            while left > 0 and not (community_section := _get_community_section(left)):
                left -= 1
            right = mid + 1
            # right에 대한 community_section은 저장할 필요가 없다.
            while not (_get_community_section(right)):
                right += 1
            if left == 0:
                left = right
        else:
            left = mid
            right = mid

        post_list = community_section.select("ul li")
        if not post_list:
            return _binary_search(start, left - 1)

        page_date = _parse_post_date(post_list[-1])
        if page_date <= end_date:
            return _binary_search(start, left - 1)

        return _binary_search(right + 1, end)

    chunk_limit = _INF

    for chunk in range(1, chunk_limit):
        start_chunk = (chunk - 1) * chunk_size + 1
        end_chunk = chunk * chunk_size
        result = _binary_search(start_chunk, end_chunk)
        if result <= end_chunk:
            return result

    return chunk_size * chunk_limit


def _parse_post_infos_per_page(
    soup: BeautifulSoup, start_date: str, end_date: str
) -> list[tuple[str, str]] | None:
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
    post_infos = []

    start_date = start_datetime.split("T")[0]
    end_date = end_datetime.split("T")[0]

    start_page = _find_start_page(keyword, start_date, end_date)

    for page in range(start_page, _INF):
        html = _fetch_search_result(keyword, page, start_date)
        soup = _get_soup(html)
        post_infos_per_page = _parse_post_infos_per_page(soup, start_date, end_date)
        if post_infos_per_page is None:
            continue
        if len(post_infos_per_page) == 0:
            break
        post_infos.extend(post_infos_per_page)

    return post_infos
