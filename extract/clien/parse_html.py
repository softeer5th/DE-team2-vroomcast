from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime

# 태그 및 CSS 셀렉터 상수 정의
CONTENT_VIEW_TAG = 'div.content_view'
TITLE_TAG = 'h3.post_subject'
DATE_TAG = 'span.view_count.date'
ARTICLE_TAG = 'div.post_article'
VIEW_COUNT_TAG = '.view_count strong'
UPVOTE_COUNT_TAG = 'a.symph_count strong'
COMMENT_COUNT_TAG = 'a.post_reply'
COMMENT_TAG = 'div.comment_row'


def extract_optional_text(element, selector, allow_empty=False):
    """
    Extracts text from an element matching a CSS selector.
    
    Searches for a sub-element within the given element using the specified CSS selector.
    Strips whitespace from the located element's text and, if the resulting text is empty
    and allow_empty is False, returns None.
    
    Args:
        element (bs4.element.Tag): The BeautifulSoup element to search within.
        selector (str): A CSS selector used to locate the sub-element.
        allow_empty (bool, optional): If False and the stripped text is empty, returns None.
                                      Defaults to False.
    
    Returns:
        Optional[str]: The stripped text from the sub-element, or None if not found 
                       or empty when not allowed.
    
    Example:
        >>> from bs4 import BeautifulSoup
        >>> html = "<div><p> Hello World </p></div>"
        >>> soup = BeautifulSoup(html, "html.parser")
        >>> extract_optional_text(soup, "p")
        "Hello World"
    """
    selected = element.select_one(selector)
    if selected:
        text = selected.text.strip()
        if not allow_empty and not text:
            return None
        return text
    return None


def normalize_text(text):
    """
    Normalize input text by replacing extra whitespace and encoding artifacts.
    
    Args:
        text (str): The text string to normalize. May contain extraneous whitespace or non-breaking
                    space characters.
    
    Returns:
        str: The cleaned text string with all consecutive whitespace replaced by a single space.
             Returns None if the input is None.
    
    Example:
        >>> normalize_text('Hello\xa0   World')
        'Hello World'
    """
    return re.sub(r'\s+', ' ', text.replace("\xa0", " ")) if text else None


def get_post_dict(html_file, file_id, url):
    """
    Extracts post details and comment metadata from HTML content.
    
    Parses the HTML using BeautifulSoup to locate and extract the post's title, creation date,
    article content, view count, upvote count, and comment count from predefined CSS selectors.
    Iterates over comment elements to construct a list of comment dictionaries with details such as
    comment ID, content, reply status, timestamp, and upvote count. Returns a dictionary containing
    all extracted data or None if critical elements are missing or an error occurs during parsing.
    
    Args:
        html_file (str): Raw HTML string of the post.
        file_id (int): Unique identifier of the post.
        url (str): URL of the post.
    
    Returns:
        dict or None: Dictionary with keys:
            - "post_id" (int): Post identifier.
            - "post_url" (str): URL of the post.
            - "title" (str): Post title.
            - "content" (str): Normalized article content.
            - "created_at" (str): ISO 8601 formatted creation date.
            - "view_count" (int): Number of views, or None.
            - "upvote_count" (int): Number of upvotes, or None.
            - "downvote_count": Always None.
            - "comment_count" (int): Number of comments, or None.
            - "comments" (list): List of dictionaries with comment details.
        Returns None if parsing fails.
    
    Notes:
        Parsing errors and missing elements are logged. No exceptions are raised.
    """
    try:
        soup = BeautifulSoup(html_file, "html.parser")

        # 콘텐츠 파싱 시작
        content = soup.select_one(CONTENT_VIEW_TAG)
        if not content:
            logging.warning(f"'content_view' 태그를 찾을 수 없습니다.")
            return None

        # 제목 추출
        title_element = content.select_one(f'{TITLE_TAG} span:nth-of-type(2)')
        content_title = title_element.text.strip() if title_element else None
        if not content_title:
            logging.warning(f"게시글 제목을 찾을 수 없습니다.")

        # 날짜 추출
        content_date = extract_optional_text(content, DATE_TAG)
        if content_date:
            content_date = ' '.join(content_date.split()[:2])
            content_date = datetime.strptime(content_date, "%Y-%m-%d %H:%M:%S").isoformat()
        else:
            logging.warning(f"게시글 날짜를 찾을 수 없습니다.")

        # 본문, 작성자, 댓글 등 추출
        content_article = normalize_text(extract_optional_text(content, ARTICLE_TAG, allow_empty=True))

        # 숫자 값 처리
        view_count = extract_optional_text(content, VIEW_COUNT_TAG)
        upvote_count = extract_optional_text(content, UPVOTE_COUNT_TAG)
        comment_count = extract_optional_text(content, COMMENT_COUNT_TAG)
        comment_tags = content.select(COMMENT_TAG)
        """
        {
			comment_id: int
			content: txt
			is_reply: bool
			created_at: (datetime)
			upvote_count: (int)
			downvote_count: (int)
		}
        """
        comment_dict_list = []
        for comment_tag in comment_tags:
            if "blocked" in comment_tag['class']:
                continue
            comment_id = comment_tag['data-comment-sn']
            content = normalize_text(comment_tag.select_one('div.comment_view').text)
            is_reply = "re" in comment_tag['class']
            created_at = comment_tag.select_one('span.timestamp').text.strip()
            created_at = ' '.join(created_at.split(' ')[:2]) if created_at else None
            created_at = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").isoformat() if created_at else None
            upvote_count = int(comment_tag.select_one('button.comment_symph').text)
            comment_dict_list.append({
                "comment_id": int(comment_id),
                "content": content,
                "is_reply": is_reply,
                "created_at": created_at,
                "upvote_count": upvote_count,
                "downvote_count": None,
            })

        # 결과 딕셔너리 생성
        post_dict = {
            "post_id": int(file_id),
            "post_url": url,
            "title": content_title,
            "content": content_article,
            "created_at": content_date,
            "view_count": int(view_count.replace(',', '')) if view_count else None,
            "upvote_count": int(upvote_count) if upvote_count == 0 or upvote_count else None,
            "downvote_count": None,
            "comment_count": int(comment_count) if comment_count else None,
            "comments": comment_dict_list,
        }
        return post_dict

    except Exception as e:
        logging.error(f"파싱 중 오류 발생(file_id: {file_id}): {e}", exc_info=True)
        return None