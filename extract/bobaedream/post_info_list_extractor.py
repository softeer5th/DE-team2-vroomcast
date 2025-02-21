from collections import OrderedDict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

_INF = 999999999


def _get_soup(html: str) -> BeautifulSoup:
    """
    Convert HTML string to a BeautifulSoup object.
    
    Args:
        html (str): A string containing HTML content.
    
    Returns:
        BeautifulSoup: An object representing the parsed HTML.
    """
    return BeautifulSoup(html, "html.parser")


_search_result_cache: OrderedDict[tuple[str, int], str] = OrderedDict()


def _cache_search_result(keyword: str, page: int, html: str) -> None:
    """
    Caches search result HTML using FIFO eviction in a limited-size cache.
    
    Args:
        keyword (str): Search term corresponding to the cached result.
        page (int): Page number associated with the result.
        html (str): HTML content of the search result to be cached.
    
    Returns:
        None.
    
    Side Effects:
        Updates the global search result cache by removing the oldest entry when the
        cache contains 20 or more entries.
    """
    if len(_search_result_cache) >= 20:
        _search_result_cache.popitem(last=False)  # FIFO
    _search_result_cache[(keyword, page)] = html


def _fetch_search_result(keyword: str, page: int, start_date: str) -> str:
    """
    Retrieve HTML content for a search query using a POST request.
    
    Checks an internal cache for the specified keyword and page; if absent, sends a POST
    request to the Bobaedream search endpoint with parameters including start date and keyword.
    Caches the result and raises an HTTPError for unsuccessful responses.
    
    Args:
        keyword (str): Search term to query posts.
        page (int): Page number of search results.
        start_date (str): Starting date for filtering search results.
    
    Returns:
        str: HTML content of the search result page.
    
    Raises:
        HTTPError: If the HTTP POST request returns an unsuccessful status code.
    
    Example:
        >>> html = _fetch_search_result("announcement", 1, "2025-01-01")
    """
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
    """
    Extracts and formats the post date from a BeautifulSoup tag.
    
    Retrieves the text from the last <span> element within an element having the CSS class "path".
    Removes any whitespace, replaces periods with hyphens, and prefixes the result with "20" to
    convert a two-digit year format into a four-digit year, yielding a "YYYY-MM-DD" formatted date.
    
    Args:
        post_tag (Tag): A BeautifulSoup tag containing the post's HTML structure.
    
    Returns:
        str: A formatted date string in the "YYYY-MM-DD" format.
    """
    post_date_raw = post_tag.select_one(".path span:last-child").text
    return "20" + "".join(post_date_raw.split()).replace(".", "-")


def _parse_post_url(post_tag: Tag) -> str:
    """
    Extracts the absolute URL of a post from its HTML tag.
    
    Args:
        post_tag (Tag): A BeautifulSoup tag representing a post, expected to include an <a> element within a <dt>.
    
    Returns:
        str: The full URL of the post, created by joining the base URL "https://www.bobaedream.co.kr" with the relative URL found in the <a> tag.
    
    Raises:
        AttributeError: If the expected <a> element is not present in the provided post_tag.
    """
    post_title_elem = post_tag.select_one("dt a")
    return urljoin("https://www.bobaedream.co.kr", post_title_elem.get("href", ""))


def _parse_post_id(post_url: str) -> int:
    """
    Extracts the numeric post ID from a URL string.
    
    Retrieves the post ID embedded in the URL, assuming it is provided as a query
    parameter following "No=" and preceding an "&". The function converts the
    extracted value to an integer.
    
    Parameters:
        post_url (str): The URL string that contains the post ID in the expected format.
    
    Returns:
        int: The post ID extracted from the URL.
    
    Raises:
        IndexError: If the URL does not contain "No=" or the expected structure is not met.
        ValueError: If the extracted ID cannot be converted to an integer.
    """
    return int(post_url.split("No=")[1].split("&")[0])


def _parse_post_info(post_tag: Tag) -> dict:
    """
    Extracts post details from the given HTML tag.
    
    Extracts the post URL, identifier, and date by invoking helper methods. The URL is obtained
    using _parse_post_url, the identifier is derived from the URL via _parse_post_id, and the post date
    is retrieved using _parse_post_date. Returns a dictionary containing the post's unique identifier,
    its URL, and the formatted date.
    
    Args:
        post_tag (Tag): BeautifulSoup Tag object representing the post element.
    
    Returns:
        dict: A dictionary with the following keys:
            - "id" (int): Unique identifier extracted from the post URL.
            - "url" (str): The absolute URL of the post.
            - "date" (str): The formatted date string of the post.
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
    Find starting page containing relevant posts.
    
    Determines the page number from which community posts fall within the specified date range by performing a binary search over paginated search results. The search is conducted in chunks of pages (default size is 500), and for each page the community section is extracted and its posts are evaluated by date. The algorithm recursively adjusts the search range, ensuring that the returned page is the first where the posts are recent enough (i.e. later than the provided end_date).
    
    Args:
        keyword (str): Keyword used to filter community posts.
        start_date (str): Lower bound date used in fetching search results.
        end_date (str): Upper bound date; pages with posts older than or equal to this date are bypassed.
        chunk_size (int, optional): Number of pages to process per binary search chunk. Defaults to 500.
    
    Returns:
        int: The page number marking the start of posts within the specified date range.
    
    Note:
        HTTP errors during search result fetching will raise exceptions.
    """
    def _get_community_section(page: int) -> Tag | None:
        html = _fetch_search_result(keyword, page, start_date)
        soup = _get_soup(html)
        community_section = soup.select_one(".search_Community")
        return community_section

    def _binary_search(start: int, end: int) -> int:
        """
        Performs a binary search to determine the starting page where posts are newer than the target end date.
        
        Recursively searches within the specified page range [start, end] to locate the boundary at which
        the last post's date transitions relative to a given end date (captured from the outer scope). The
        routine retrieves the community section via `_get_community_section`. If a valid section is not found
        at mid, it scans adjacent pages (both left and right) to locate the nearest page containing posts.
        Once a community section is found, the function examines the date of the last post—using `_parse_post_date`—
        and decides whether to continue the search to the left or right accordingly.
        
        Args:
            start (int): Lower bound of the page range.
            end (int): Upper bound of the page range.
        
        Returns:
            int: The determined page number marking the transition based on the end date.
        """
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
    """
    Parses and filters post information from a BeautifulSoup object by date range.
    
    Extracts the community section from the provided HTML soup and iterates over each
    post element. Each post is processed via the helper function `_parse_post_info`, and
    only posts whose date falls between start_date and end_date (inclusive) are kept.
    Returns None if the community section is not found.
    
    Args:
        soup (BeautifulSoup): Parsed HTML content of the search results page.
        start_date (str): Inclusive lower bound for filtering posts by date.
        end_date (str): Inclusive upper bound for filtering posts by date.
    
    Returns:
        list[dict] or None: A list of post information dictionaries containing details
        such as post ID, URL, and date, or None if the community section is missing.
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
    Retrieves post information for a given keyword within a specified date range.
    
    Determines the starting page of posts using binary search, then iterates
    through successive pages to aggregate post details from the community. The 
    date filter is applied using the date component of the provided ISO-formatted 
    start and end datetime strings.
    
    Args:
        keyword (str): The search keyword to filter community posts.
        start_datetime (str): An ISO-formatted datetime string (e.g., "YYYY-MM-DDTHH:MM:SS")
            representing the beginning of the date range.
        end_datetime (str): An ISO-formatted datetime string (e.g., "YYYY-MM-DDTHH:MM:SS")
            representing the end of the date range.
    
    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains details of a post,
        such as its ID, URL, and date.
    
    Raises:
        requests.HTTPError: If an HTTP error occurs during retrieval of search results.
    
    Example:
        >>> posts = get_post_infos("community", "2025-01-01T00:00:00", "2025-01-31T23:59:59")
        >>> if posts:
        ...     print(posts[0]["url"])
    """
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
