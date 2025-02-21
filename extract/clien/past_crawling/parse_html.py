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
    Extract text from the first matching element using a CSS selector.
    
    Args:
        element (bs4.element.Tag): The BeautifulSoup element to search within.
        selector (str): The CSS selector used to locate the target element.
        allow_empty (bool, optional): If False, returns None when the extracted text is empty.
            Defaults to False.
    
    Returns:
        Optional[str]: The stripped text if present and valid, otherwise None.
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
    텍스트 정규화를 수행하여 불필요한 공백 및 특정 인코딩 문자를 제거합니다.
    
    문자열 내의 연속된 공백(스페이스, 탭 등)을 단일 공백으로 축소하고, 넌브레이킹 스페이스(\\xa0)를 일반 공백으로 변환합니다.
    입력 값이 None 또는 빈 문자열인 경우 None을 반환합니다.
    
    매개변수:
        text (str or None): 정규화할 문자열.
    
    반환:
        str or None: 정리된 문자열, 입력값이 None 또는 빈 문자열인 경우 None.
    """
    return re.sub(r'\s+', ' ', text.replace("\xa0", " ")) if text else None


def get_post_dict(html_file, file_id, url):
    """
    Extracts post details, metadata, and comments from HTML content.
    
    Parses the provided HTML string using BeautifulSoup to retrieve the title, 
    creation date, article content, and interaction metrics (view count, upvotes, 
    comment count). Valid comments are processed into a list of dictionaries, each 
    containing the comment ID, content, reply status, creation timestamp, upvote 
    count, and a placeholder for downvote count. Returns a structured dictionary 
    with all extracted data or None if required elements are missing or an error 
    occurs during parsing.
    
    Args:
        html_file (str): HTML content of the post.
        file_id (int): Unique identifier of the post/file.
        url (str): URL of the post.
    
    Returns:
        dict or None: A dictionary with keys "post_id", "post_url", "title", 
        "content", "created_at", "view_count", "upvote_count", "downvote_count", 
        "comment_count", and "comments" (a list of comment dictionaries) if parsing 
        succeeds; otherwise, None.
    
    Example:
        >>> html_content = '<html><body><div class="content_view">...</div></body></html>'
        >>> result = get_post_dict(html_content, 101, 'http://example.com/post/101')
        >>> print(result)
        {'post_id': 101, 'post_url': 'http://example.com/post/101', 'title': 'Sample Title', ...}
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