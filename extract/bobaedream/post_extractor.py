import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup


def _fetch_post(url: str) -> str | None:
    """
    Fetch HTML content from a URL.
    
    Initiates an HTTP GET request with custom headers and a defined timeout to retrieve
    the HTML content of the specified URL. Returns the HTML text encoded in UTF-8 if the
    request is successful; otherwise, prints an error message and returns None.
    
    Args:
        url (str): URL of the web post to fetch.
    
    Returns:
        str | None: The HTML content as a string if successful, or None on failure.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Referer": "https://www.bobaedream.co.kr/search",
        }
        response = requests.get(url, headers=headers, timeout=(3, 10))
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text
    except Exception as e:
        print(f"Error fetching post: {e}")
        return None


def _get_soup(html: str) -> BeautifulSoup:
    """
    Converts HTML content into a BeautifulSoup object.
    
    Args:
        html (str): A string containing HTML markup to be parsed.
    
    Returns:
        BeautifulSoup: A BeautifulSoup object representing the parsed HTML.
    """
    return BeautifulSoup(html, "html.parser")


def _convert_to_iso_format(date_str: str) -> str:
    """
    Convert a date string to ISO 8601 format.
    
    Expects the date string to contain a date in the format "YY.MM.DD <separator> HH:MM" or
    "YYYY.MM.DD <separator> HH:MM". Two-digit years are assumed to be in the 2000s and are
    converted to their four-digit representation. Returns the ISO 8601 formatted date and time
    if parsing is successful; otherwise, returns an empty string.
    
    Args:
        date_str (str): A string representing a date and time, for example "21.12.05 03:30".
    
    Returns:
        str: The date and time in ISO 8601 format ("YYYY-MM-DDTHH:MM:SS") or an empty string if
             parsing fails.
    """
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
    """
    Extracts the post title from parsed HTML content.
    
    Args:
        soup (BeautifulSoup): Parsed HTML document.
    
    Returns:
        str: The trimmed title text truncated at the first '[' character, or an empty string if not found.
    """
    title_elem = soup.select_one(".writerProfile dt strong")
    if title_elem:
        title = title_elem.get_text().split("[")[0].strip()
        return title
    return ""


def _parse_post_content(soup: BeautifulSoup) -> str:
    """
    Extracts the main post content from the provided BeautifulSoup object.
    
    Args:
        soup (BeautifulSoup): Parsed HTML containing the post data.
    
    Returns:
        str: The cleaned text content of the post if found, or an empty string.
    """
    content_elem = soup.select_one(".bodyCont")
    if content_elem:
        return content_elem.get_text(strip=True)
    return ""


def _parse_post_created_at(soup: BeautifulSoup) -> str:
    """
    Extracts the post creation date and converts it to ISO 8601 format.
    
    Inspects the specified HTML element for a date string formatted like
    "조회 433 | 추천 0 | 2025.02.10 (월) 08:25", extracts the date portion,
    and converts it using _convert_to_iso_format. Returns an empty string if no
    date information is found.
    
    Args:
        soup (BeautifulSoup): Parsed HTML content of the post.
    
    Returns:
        str: ISO 8601 formatted creation date or an empty string if unavailable.
    """
    date_elem = soup.select_one(".writerProfile .countGroup")
    if date_elem:
        # extract date from text like "조회 433 | 추천 0 | 2025.02.10 (월) 08:25"
        date_text = date_elem.get_text().split("|")[-1].strip()
        create_at = _convert_to_iso_format(date_text)
        return create_at
    return ""


def _parse_post_view_count(soup: BeautifulSoup) -> int:
    """
    Extracts the view count from the provided BeautifulSoup object.
    
    Parses the text of the first HTML element that matches the CSS selector 
    ".writerProfile .countGroup .txtType" and converts it to an integer. Returns 0 
    if the element is not found or if the conversion fails.
    
    Args:
        soup (BeautifulSoup): Parsed HTML content.
    
    Returns:
        int: The extracted view count, or 0 if parsing is unsuccessful.
    """
    view_elem = soup.select_one(".writerProfile .countGroup .txtType")
    if view_elem:
        try:
            return int(view_elem.get_text().strip())
        except ValueError:
            return 0
    return 0


def _parse_post_upvote_count(soup: BeautifulSoup) -> int:
    """
    Extracts the upvote count from the parsed post HTML.
    
    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the post's HTML.
    
    Returns:
        int: The upvote count as an integer. Returns 0 if the element is missing or
             the conversion to an integer fails.
    """
    upvote_elem = soup.select(".writerProfile .countGroup .txtType")[1]
    if upvote_elem:
        try:
            return int(upvote_elem.get_text().strip())
        except (ValueError, IndexError):
            return 0
    return 0


def _parse_post_comment_count(soup: BeautifulSoup) -> int:
    """
    Extracts the comment count from the BeautifulSoup object.
    
    Args:
        soup (BeautifulSoup): Parsed HTML content containing the comment information.
    
    Returns:
        int: Number of comments parsed from the element. Returns 0 if the element is missing or the count cannot be converted to an integer.
    """
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
    """
    Parses a comment element from a BeautifulSoup tag.
    
    Extracts key details from the comment HTML including the comment ID, content, creation
    date (converted to ISO 8601 format), upvote and downvote counts, and whether the comment
    is a reply. Defaults are used when any expected element is absent.
    
    Args:
        comment_tag (BeautifulSoup): A BeautifulSoup element representing a comment.
    
    Returns:
        dict[str, str | int]: A dictionary containing:
            - "comment_id" (int): The numeric identifier extracted from the comment element.
            - "content" (str): The text content of the comment.
            - "created_at" (str): The comment's creation date in ISO 8601 format.
            - "upvote_count" (int): Count of upvotes for the comment.
            - "downvote_count" (int): Count of downvotes for the comment.
            - "is_reply" (bool): True if the comment is a reply, otherwise False.
    """

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
    """
        Parse all comments from the BeautifulSoup object.
    
        Searches for the <ul> element with class "basiclist" and id "cmt_reply" containing comment items.
        Iterates over each <li> element within the list, skipping those that lack a <dt> element, and uses
        _parse_comment to extract individual comment details. Returns a list of dictionaries with parsed
        comment data.
    
        Args:
            soup: BeautifulSoup
                The parsed HTML content from which to extract the comment list.
    
        Returns:
            A list of dictionaries containing comment information. Each dictionary includes keys such as
            comment ID, content, creation date, and vote counts. Returns an empty list if the comment list
            element is not found.
        """
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


def _parse_post(soup: BeautifulSoup, url: str, id: str, start_datetime: str, end_datetime: str) -> dict[str, str | int] | None:
    """
    Parses and extracts post details from a BeautifulSoup object if the creation date falls within the specified range.
    
    Validates the post's creation date using the provided start and end datetime bounds. If the post was created outside this range, returns None. Otherwise, extracts the title, content, view count, upvote count, comment count, and comments, organizing them in a dictionary. The downvote count is always set to None due to unavailability.
    
    Args:
        soup (BeautifulSoup): Parsed HTML content of the post.
        url (str): URL of the post.
        id (str): Unique identifier for the post.
        start_datetime (str): Lower bound of the allowed creation date in ISO format.
        end_datetime (str): Upper bound of the allowed creation date in ISO format.
    
    Returns:
        dict[str, str | int] | None: Dictionary containing:
            "post_id": Post identifier.
            "post_url": URL of the post.
            "title": Title of the post.
            "content": Main content of the post.
            "created_at": Post creation date in ISO format.
            "view_count": Number of views.
            "upvote_count": Number of upvotes.
            "downvote_count": Always None.
            "comment_count": Number of comments.
            "comments": List of parsed comment data.
        Returns None if the post's creation date is outside the specified range.
    """
    created_at = _parse_post_created_at(soup)

    if created_at < start_datetime or created_at > end_datetime:
        return None

    title = _parse_post_title(soup)
    content = _parse_post_content(soup)
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


def extract_post(url: str, id: str, start_datetime: str, end_datetime: str) -> tuple[dict[str, str | int], bool]:
    """
    Extracts and parses a web post from the specified URL.
    
    Fetches the HTML content using a GET request with custom headers and converts it
    to a BeautifulSoup object for parsing. Extracts details such as title, content,
    creation date, view count, upvote count, comment count, and comments. Returns
    a tuple containing the parsed post data and a boolean flag indicating success.
    If fetching the HTML fails, returns (None, False).
    
    Args:
        url (str): The URL of the web post.
        id (str): Identifier associated with the post.
        start_datetime (str): Inclusive lower bound for the post creation date (ISO 8601).
        end_datetime (str): Inclusive upper bound for the post creation date (ISO 8601).
    
    Returns:
        tuple[dict[str, str | int] | None, bool]:
            A tuple where the first element is a dictionary with the parsed post data
            (or None if the post could not be retrieved) and the second element is a boolean
            indicating the success of the extraction.
    
    Example:
        >>> result, success = extract_post("http://example.com/post/123", "123",
        ...                                 "2025-01-01T00:00:00", "2025-01-31T23:59:59")
        >>> if success:
        ...     print(result["title"])
    """
    html = _fetch_post(url)

    if not html:
        return None, False

    soup = _get_soup(html)
    return _parse_post(soup, url, id, start_datetime, end_datetime), True
