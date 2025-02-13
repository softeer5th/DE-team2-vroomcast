from bs4 import BeautifulSoup
import re
import logging
import json

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
    """선택 요소에서 텍스트를 추출하거나 None 반환"""
    selected = element.select_one(selector)
    if selected:
        text = selected.text.strip()
        if not allow_empty and not text:
            return None
        return text
    return None


def normalize_text(text):
    """텍스트 정규화: 여분의 공백 및 인코딩 제거"""
    return re.sub(r'\s+', ' ', text.replace("\xa0", " ")) if text else None


def get_post_dict(html_file, file_id, url):
    """
    Parses the provided HTML content to extract post details along with associated metadata
    and comments. The function processes elements inside a predefined content view tag,
    extracting the post's title, date of creation, article content, and interaction metrics
    such as view count, upvotes, and comment count. It also processes and structures the
    comments into a list of dictionaries containing individual comment data like content,
    reply status, timestamps, and upvote counts.

    :param html_file: HTML content of the post
    :type html_file: str
    :param file_id: Unique identifier of the file or post
    :type file_id: int
    :param url: URL of the post
    :type url: str
    :return: A dictionary containing post details and comments or None if parsing fails
    :rtype: dict or None
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
            comment_id = comment_tag['data-comment-sn']
            content = normalize_text(comment_tag.select_one('div.comment_view').text)
            is_reply = "re" in comment_tag['class']
            created_at = comment_tag.select_one('span.timestamp').text.strip()
            upvote_count = int(comment_tag.select_one('button.comment_symph').text)
            comment_dict_list.append({
                "comment_id": int(comment_id),
                "content": content,
                "is_reply": is_reply,
                "created_at": created_at,
                "upvote_count": upvote_count,
            })

        # 결과 딕셔너리 생성
        post_dict = {
            "post_id": int(file_id),
            "post_url": url,
            "title": content_title,
            "content": content_article,
            "created_at": content_date,
            "view_count": int(view_count.replace(',', '')) if view_count else None,
            "upvote_count": int(upvote_count) if upvote_count else None,
            "comment_count": int(comment_count) if comment_count else None,
            "comments": comment_dict_list,
        }
        return post_dict

    except Exception as e:
        logging.error(f"파싱 중 오류 발생(file_id: {file_id}): {e}", exc_info=True)
        return None