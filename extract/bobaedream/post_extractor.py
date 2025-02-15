import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup


def _fetch_post(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Referer": "https://www.bobaedream.co.kr/search",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text


def _get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _convert_to_iso_format(date_str: str) -> str:
    """날짜를 ISO 형식으로 변환"""
    pattern = r"(\d+)\.(\d+)\.(\d+)\D+(\d+):(\d+)"
    match = re.search(pattern, date_str)
    if not match:
        return ""

    year, month, day, hour, minute = match.groups()

    # 2자리 연도를 4자리로 변환
    if len(year) == 2:
        year = "20" + year

    try:
        dt = datetime(
            year=int(year),
            month=int(month),
            day=int(day),
            hour=int(hour),
            minute=int(minute),
        )
        return dt.isoformat(timespec="seconds")
    except ValueError:
        return ""


def _parse_post_title(soup: BeautifulSoup) -> str:
    """게시글 제목 파싱"""
    title_elem = soup.select_one(".writerProfile dt strong")
    if title_elem:
        title = title_elem.get_text().split("[")[0].strip()
        return title
    return ""


def _parse_post_content(soup: BeautifulSoup) -> str:
    """게시글 본문 내용 파싱"""
    content_elem = soup.select_one(".bodyCont")
    if content_elem:
        return content_elem.get_text(strip=True)
    return ""


def _parse_post_created_at(soup: BeautifulSoup) -> str:
    """작성 날짜 파싱"""
    date_elem = soup.select_one(".writerProfile .countGroup")
    if date_elem:
        # extract date from text like "조회 433 | 추천 0 | 2025.02.10 (월) 08:25"
        date_text = date_elem.get_text().split("|")[-1].strip()
        create_at = _convert_to_iso_format(date_text)
        return create_at
    return ""


def _parse_post_view_count(soup: BeautifulSoup) -> int:
    """조회수 파싱"""
    view_elem = soup.select_one(".writerProfile .countGroup .txtType")
    if view_elem:
        try:
            return int(view_elem.get_text().strip())
        except ValueError:
            return 0
    return 0


def _parse_post_upvote_count(soup: BeautifulSoup) -> int:
    """추천수 파싱"""
    upvote_elem = soup.select(".writerProfile .countGroup .txtType")[1]
    if upvote_elem:
        try:
            return int(upvote_elem.get_text().strip())
        except (ValueError, IndexError):
            return 0
    return 0


def _parse_post_comment_count(soup: BeautifulSoup) -> int:
    """댓글 수 파싱"""
    comment_elem = soup.select_one(".writerProfile dt strong em")
    if comment_elem:
        try:
            # remove brackets from text like "[5]"
            count = comment_elem.get_text().strip("[]")
            return int(count)
        except ValueError:
            return 0
    return 0


def _parse_comment(comment_tag: BeautifulSoup) -> dict[str, str | int]:
    """댓글 파싱"""

    content_tag = comment_tag.select_one("dd")
    dt_tag = comment_tag.select_one("dt")

    # 댓글 ID
    comment_id_text = content_tag.get("id")
    comment_id = int(re.search(r"\d+", content_tag.get("id")).group())

    # 작성 날짜
    date = dt_tag.select_one(".date")
    date_text = date.get_text(strip=True) if date else ""
    created_at = _convert_to_iso_format(date_text)

    # 댓글 내용
    content_tag = comment_tag.select_one("dd")
    content = content_tag.get_text(strip=True) if content_tag else ""

    # 추천/반대 수
    updown_box = comment_tag.select_one(".updownbox")
    if updown_box:
        # "추천 2", "반대 3" 형태에서 숫자만 추출
        upvotes = updown_box.select_one(".first")
        upvote_count = int(upvotes.get_text().split()[-1]) if upvotes else 0

        downvotes = updown_box.select_one(".last")
        downvote_count = int(downvotes.get_text().split()[-1]) if downvotes else 0
    else:
        upvote_count = downvote_count = 0

    # 대댓글 여부
    is_reply = bool(comment_tag.get("class") and "re" in comment_tag.get("class"))

    return {
        "comment_id": comment_id,
        "content": content,
        "created_at": created_at,
        "upvote_count": upvote_count,
        "downvote_count": downvote_count,
        "is_reply": is_reply,
    }


def _parse_comments(soup: BeautifulSoup) -> list[dict[str, str | int]]:
    """전체 댓글 목록 파싱"""
    comments = []

    comment_list = soup.select_one("ul.basiclist#cmt_reply")
    if not comment_list:
        return comments

    for comment_tag in comment_list.find_all("li"):
        if not comment_tag.select_one("dt"):
            continue
        comment = _parse_comment(comment_tag)
        comments.append(comment)

    return comments


def _parse_post(soup: BeautifulSoup, url: str, id: str) -> dict[str, str | int]:
    title = _parse_post_title(soup)
    content = _parse_post_content(soup)
    created_at = _parse_post_created_at(soup)
    view_count = _parse_post_view_count(soup)
    upvote_count = _parse_post_upvote_count(soup)
    downvote_count = None
    comment_count = _parse_post_comment_count(soup)
    comments = _parse_comments(soup)

    return {
        "post_id": id,
        "post_url": url,
        "title": title,
        "content": content,
        "created_at": created_at,
        "view_count": view_count,
        "upvote_count": upvote_count,
        "downvote_count": downvote_count,
        "comment_count": comment_count,
        "comments": comments,
    }


def extract_post(url: str, id: str) -> dict[str, str | int]:
    html = _fetch_post(url)
    soup = _get_soup(html)
    return _parse_post(soup, url, id)
