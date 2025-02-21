import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from openai import OpenAI
import kss
import pandas as pd
import os
import argparse
import logging
import openai
import time

logger = logging.getLogger(__name__)

# Pandas UDF의 반환 타입을 StructType으로 지정
keyword_dict = {
  "design": [
    "디자인","실내","실외","내장","외장","전면부","후면부","루프라인","휠","시트",
    "재질","범퍼","형상","크롬","몰딩","투톤","컬러","대시보드","센터페시아","계기판",
    "핸들","암레스트","도어트림","내장재","마감","상태","헤드램프","테일램프","안개등","LED",
    "라이트","HID","DRL","라디에이터","그릴","사이드미러","루프","스포일러","리어","사이드",
    "스커트","익스테리어","인테리어","페이스리프트","풀체인지","패밀리룩","고급감","라인","배색","우드트림",
    "스티어링휠","가죽","패턴","나파","착좌감","공간감","파노라마","선루프","썬루프","무드등",
    "완성도","측면","실루엣","바디킷","에어댐","공기역학","루프랙","하이글로시","바디","대형",
    "프로젝터","램프","우아함","와이드","헤드업","디스플레이","버킷","앰비언트","에어벤트","조명",
    "인조가죽","메탈","트림","스티치","포인트","블랙베젤","카본","하차감","스포티","룩",
    "다이내믹","가니쉬","수평","기조","긴","휠베이스","박스","스타일","슬림","스플리터"
  ],
  "performance": [
    "성능","출력","토크","마력","속도","가속력","제동력","브레이크","승차감","핸들링",
    "코너링","서스펜션","쇼바","하체","엔진","미션","변속기","수동","자동","CVT",
    "DCT","변속","타이밍","패들시프트","파워트레인","터보","슈퍼차저","자연흡기","하이브리드","PHEV",
    "전기차","주행","연비","주행거리","배기음","배기가스","RPM","엔진음","소음","진동",
    "트랙션","그립감","사륜구동","후륜구동","전륜구동","AWD","4WD","LSD","자율주행","ADAS",
    "차선","유지","어댑티브","크루즈","HUD","TCS","ESC","ABS","언더스티어","오버스티어",
    "반응","속도","가속","페달","브레이크페달","오토홀드","재시동","에코","모드","스포츠",
    "N모드","드라이브","컴포트","맵핑","튜닝","흡기","인젝터","인터쿨러","하이브리드시스템","모터",
    "출력","등판","능력","고속","시내","장거리","차체","강성","충격","흡수",
    "드리프트","발열","냉각","수온","파워","브렘보","제로백","크루즈컨트롤","내구성",
    "네비게이션","반응","가속도","반자율","코너","스로틀","감도","스티어링","피드백","차고", "GMP"
  ],
  "price": [
    "가격","고가","저가","가성비","가심비","창렬","혜자","트림","깡통","풀옵션",
    "돈값","돈낭비","유지비","정비비","수리비","부품비","보험료","세금","취득세","등록세",
    "개소세","부가세","고정비","변동비","할부","리스","렌트","중고차","중고","시세",
    "감가","잔존","가치","보증","연장","기간","할인이벤트","프로모션","특판","출고가",
    "실구매가","계약금","잔금","월납입금","잔가","영맨","할인","파이낸스","이자","금리",
    "고정","변동","무이자","포인트","캐시백","보조금","수소차","폐차","리세일",
    "매도","매입","딜러","매물","호갱","깎기","가격협상","옵션가","악옵","옵션질",
    "유료옵션","결제","카드","현금","견적","사전","계약","출고","재고","무옵션",
    "할부금","인수","거부","잔가보장","잔가형","리스","부담금","중고보상","업그레이드","다운그레이드",
    "세이브","구입비","초기","유지","극할인","대차","반납","취등록세","견적서","중고매입"
  ],
  "fault": [
    "결함","고장","하자","불량","리콜","누유","누수","잡소리","이상음","소음",
    "진동","떨림","과열","시동","불량","급발진","배터리","방전",
    "경고등","배선","수리","오작동","에러코드","미션","클러치","마모","브레이크","디스크",
    "변형","오일","냉각수","흡기","배기","촉매","손상","DPF","막힘","EGR",
    "ECU","ICCU","퓨즈","단선","A/S","AS","요청","홈링","오류","과도","연소",
    "실린더","피스톤","부싱","로어암","데프","조향","얼라인먼트","유격","후방카메라","비상등",
    "에어컨","히터","와이퍼","뒷문","도어락","도장","코팅","블랙박스","운전석","누적",
    "계기판","RPM","불안정","밀림","현상","오일필터","누락","장착","에어백","실내등",
    "공조장치","진공펌프","융착","와이어","차체","부식","트렁크","오버히트","고장코드","노이즈",
    "녹","무반응","인식","키","연료계통","펌프","필터","갈림","소프트웨어","업그레이드",
    "모듈","점화","체인","늘어짐","판정","재발","위험","드라이브샤프트","파손","부품"
  ]
}

from pydantic import BaseModel, Field

# Pydantic을 활용한 감성 분석 결과 모델 정의
class SentimentAnalysis(BaseModel):
    sentiments: list[int] = Field(
        ..., description="각 문장의 감성 점수 배열 (1: 긍정, 0: 중립, -1: 부정)"
    )

def analyze_sentiments(sentences):
    # return [ random.randint(-1,1) for _ in range(len(sentences))]
    """
    주어진 한국어 문장 목록에 대해 Batch로 감성 분석을 수행합니다.
    
    입력된 문장들을 긍정(1), 부정(-1), 또는 중립(0)으로 분류하며, 각 문장에 해당하는 감성 점수를 숫자 리스트로 반환합니다. 응답 배열의 길이가 입력 문장 수와 일치하지 않거나 에러가 발생할 경우, 기본값(0)으로 채운 리스트를 반환합니다.
    
    인자:
        sentences (list[str]): 감성 분석 대상 한국어 문장 리스트. None 또는 빈 리스트일 경우 빈 리스트를 반환.
    
    반환:
        list[int]: 각 문장에 대한 감성 점수 리스트. 1은 긍정, -1은 부정, 0은 중립을 의미하며, 입력 문장 수와 동일한 길이를 갖습니다.
    
    예외 처리:
        - OpenAI API 호출 중 RateLimitError 발생 시 최대 10회까지 지수적 백오프(1.5^시도 횟수)를 적용하여 재시도합니다.
        - 기타 예외 발생 시 에러를 로깅하고 기본값(0)으로 채운 리스트를 반환합니다.
    
    참고:
        - 환경 변수 OPENAI_API_KEY가 설정되어 있어야 합니다.
    """
    if sentences is None or len(sentences) == 0:
        return []

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Batch 요청을 위한 프롬프트 생성
    prompt = "\n".join([f'{idx + 1}번째 문장 >> "{sent}"' for idx, sent in enumerate(sentences)])

    full_prompt = f"""
    다음은 한국어 문장 목록입니다. 각 문장의 감성을 긍정(1), 부정(-1), 중립(0) 중 하나로 분류하세요.
    응답은 배열 형식으로 숫자 값만 반환합니다. 
    !!! 응답의 형식은 반드시 입력된 문장의 개수({len(sentences)})와 동일한 길이의 배열이어야 합니다. !!!
    각 번호에 해당하는 감정은 다음과 같은 양식으로만 작성해야 합니다:
    [1, -1, 0, …]

    문장 개수: {len(sentences)}
    다음은 문장 목록입니다:
    {prompt}
    """
    max_retries = 10
    for attempt in range(max_retries):
        try:
            response = client.beta.chat.completions.parse(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system",
                     "content": "당신의 작업은 주어진 한국어 문장의 감성을 긍정(1), 부정(-1), 중립(0)으로 분류하고 배열 형식으로 숫자 값만 반환하는 것입니다."},
                    {"role": "user", "content": full_prompt},
                ],
                response_format=SentimentAnalysis,
            )
            sentiments = response.choices[0].message.parsed.sentiments
            print(sentiments)
            # 응답 개수가 입력 개수와 다를 경우 기본값 반환
            if len(sentiments) != len(sentences):
                logger.warning(f"[Error Open AI] 응답 개수 불일치: 입력({len(sentences)}) vs 응답({len(sentiments)})")
                return [0] * len(sentences)

            logger.info(f"[Rated] Sentiments: {sentiments}, attempt: {attempt+1}/{max_retries}")
            return sentiments

        except openai.RateLimitError as e:
            wait_time = 1.5 ** attempt
            logger.warning(f"[Rate Limit 초과] {e}. {wait_time}초 후 재시도... ({attempt+1}/{max_retries})")
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"[Error Open AI] {e}")
            return [0] * len(sentences)

    logger.error("[Error Open AI] 최대 재시도 횟수를 초과했습니다.")
    return [0] * len(sentences)


@pandas_udf(ArrayType(
    StructType([
        StructField("id", StringType(), False),
        StructField("sentence", StringType(), True),
        StructField("from_post", BooleanType(), True)
    ])
))
def get_sentences(source_id: pd.Series, title: pd.Series, content: pd.Series) -> pd.Series:
    """
    Extracts sentences from title and content and assigns unique identifiers.
    
    Splits the text in the title and content columns into sentences using kss. For each row, sentences from the title are obtained first (if available), followed by those from the content. Each sentence is paired with a unique identifier, which combines the source identifier with the sentence index, and a flag indicating if the sentence is from the title (True if a title is provided, else False).
    
    Args:
        source_id (pd.Series): Series of identifiers for each text row.
        title (pd.Series): Series containing title text to be segmented into sentences.
        content (pd.Series): Series containing content text to be segmented into sentences.
    
    Returns:
        pd.Series: A Series where each element is a list of tuples. Each tuple contains:
            - str: A unique sentence identifier formatted as "source_id_index".
            - str: The segmented sentence.
            - bool: True if the sentence originates from the title, False otherwise.
    
    Example:
        >>> import pandas as pd
        >>> source_id = pd.Series([101, 102])
        >>> title = pd.Series(["Hello world", ""])
        >>> content = pd.Series(["This is a test.", "Content for second row."])
        >>> get_sentences(source_id, title, content)
        0    [(101_0, 'Hello world', True), (101_1, 'This is a test.', True)]
        1    [(102_0, 'Content for second row.', False)]
        dtype: object
    """
    results = []
    for source_id, title, content in zip(source_id, title, content):
        result_per_content = []
        sentences = kss.split_sentences(title) if title else []
        content_sentences = kss.split_sentences(content) if content else []
        sentences.extend(content_sentences)
        for idx, sentence in enumerate(sentences):
            result_per_content.append((f"{source_id}_{idx}", sentence, True if title else False))
        results.append(result_per_content)
    return pd.Series(results)

@pandas_udf(StructType([
    StructField("sentiment", IntegerType(), True),  # 감성 분석 점수
    StructField("categories_keywords", ArrayType(  # 카테고리 키워드 리스트
        StructType([
            StructField("category", StringType(), True),
            StructField("keyword", StringType(), True)
        ])
    ), True)
]))
def extract_sentiment_category_keyword(sentences: pd.Series) -> pd.Series:
    """
    Extracts sentiment scores and associated keyword categories from each sentence.
    
    Processes a pandas Series of sentences by batching them (batch size = 20) and performing sentiment
    analysis via an external API. For each sentence, the text is transformed to uppercase to facilitate
    keyword matching against a pre-defined keyword dictionary. For every detected keyword, a dictionary
    with keys "category" and "keyword" is recorded. The output is a DataFrame with each sentence’s sentiment
    and a list of its matched keyword-category pairs.
    
    Args:
        sentences (pd.Series): A Series of sentence strings to be analyzed.
    
    Returns:
        pd.DataFrame: A DataFrame with two columns:
            - "sentiment": The sentiment score or classification returned by the sentiment analysis.
            - "categories_keywords": A list of dictionaries, each containing a detected keyword and its category.
    
    Example:
        >>> import pandas as pd
        >>> sentences = pd.Series([
        ...     "The design is sleek and modern.",
        ...     "가격이 합리적입니다."
        ... ])
        >>> extract_sentiment_category_keyword(sentences)
             sentiment                      categories_keywords
        0  positive   [{'category': 'design', 'keyword': 'DESIGN'}]
        1  positive    [{'category': 'price', 'keyword': '가격'}]
    """
    results = []  # 최종 반환 값 저장
    BATCH_SIZE = 20  # Batch 크기 설정

    for batch_idx in range(0, len(sentences), BATCH_SIZE):
        # 입력 문장을 배치 단위로 분할
        batch_sentences = sentences[batch_idx:batch_idx + BATCH_SIZE]

        # 배치 내 모든 문장에 대해 감정 분석 실행
        sentiments = analyze_sentiments(batch_sentences)

        # 각 문장마다 처리
        for sentence, sentiment in zip(batch_sentences, sentiments):
            upper_sentence = sentence.upper()  # 대문자로 변환하여 키워드 검색
            categories_keywords = []  # 각 문장의 키워드 결과 저장

            # 카테고리와 키워드 탐지
            for category, keywords in keyword_dict.items():
                for keyword in keywords:
                    if keyword in upper_sentence:
                        # 키워드 탐지 시 카테고리와 키워드를 구조체로 추가
                        categories_keywords.append({"category": category, "keyword":keyword})
            # 최종적으로 문장의 sentiment와 categories_keywords를 저장
            results.append((sentiment, categories_keywords))

    return pd.DataFrame(results, columns=["sentiment", "categories_keywords"])

# 링크 필터링, 유저 필터링
def regex_replace_privacy(df):
    """
    Replaces URLs and user mentions in the "content" column of a Spark DataFrame.
    
    Performs two regex-based replacements on the "content" column:
    1. Removes any substring that matches a URL pattern (http:// or https://).
    2. Removes any substring that matches a user mention pattern starting with '@' followed by alphanumeric or Korean characters.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame containing a "content" column to sanitize.
    
    Returns:
        pyspark.sql.DataFrame: Modified DataFrame with URLs and user mentions removed from the "content" column.
    """
    df = df.withColumn("content", regexp_replace(col("content"), r'https?://\S+', ''))
    df = df.withColumn("content", regexp_replace(col("content"), r'@[\w\uac00-\ud7af]+', ''))
    return df

def transform_static_data(post_df, comment_df):
    """
    Transforms and writes posts and comments for sentiment analysis.
    
    Removes privacy-sensitive content from both posts and comments, extracts sentences using a UDF,
    applies sentiment analysis and keyword extraction on each sentence, and writes the processed data
    to multiple parquet files. The function processes posts and comments separately by:
      - Replacing URLs and user mentions using `regex_replace_privacy`.
      - Extracting sentences via the `get_sentences` UDF (with posts using the title and content, and
        comments having a null title).
      - Combining sentence-level data from posts and comments.
      - Analyzing each sentence using the `extract_sentiment_category_keyword` UDF to obtain sentiment scores
        and category keywords.
      - Caching the intermediate sentence DataFrame to improve performance.
    
    The results are written to output paths constructed using global variables (e.g., `mode`, `bucket`,
    `output_dir`), with separate outputs for post static data, comment static data, sentence-level data,
    and keyword-category mappings. Logging is performed at each major write operation.
        
    Args:
        post_df (DataFrame): Spark DataFrame containing posts with fields like id, title, content, created_at.
        comment_df (DataFrame): Spark DataFrame containing comments with fields like id, content, created_at.
    
    Side Effects:
        Outputs parquet files under directories for posts, comments, sentences, and keyword categories.
    """
    post_df = regex_replace_privacy(post_df)
    comment_df = regex_replace_privacy(comment_df)
    post_pre_sentence_df = post_df.selectExpr("id AS source_id", "title", "content", "created_at")
    comment_pre_sentence_df = comment_df.selectExpr("id AS source_id", "CAST(NULL AS STRING) AS title", "content", "created_at")

    post_sentence_df = post_pre_sentence_df.withColumn(
        "sentences",
        explode(
            get_sentences(
                col("source_id"), col("title"), col("content")
            )
        )
    ).selectExpr("sentences.id AS id", "source_id", "sentences.from_post AS from_post",
                 "sentences.sentence AS sentence", "created_at")
    comment_sentence_df = comment_pre_sentence_df.withColumn(
        "sentences",
        explode(
            get_sentences(
                col("source_id"), col("title"), col("content")
            )
        )
    ).selectExpr("sentences.id AS id", "source_id", "sentences.from_post AS from_post",
                 "sentences.sentence AS sentence", "created_at")
    sentence_df = post_sentence_df.union(comment_sentence_df)
    sentence_df_with_keywords = sentence_df.withColumn(
        "sentiment_category_keyword",
        extract_sentiment_category_keyword(col("sentence"))
    )
    sentence_df_with_keywords.cache()
    sentence_df = sentence_df_with_keywords.selectExpr("id", "source_id", "from_post", "sentence",
                                                       "sentiment_category_keyword.sentiment AS sentiment",
                                                       "created_at")
    keyword_category = sentence_df_with_keywords.selectExpr("id as sentence_id",
                                                            "sentiment_category_keyword.categories_keywords AS categories_keywords")
    keyword_category = keyword_category.withColumn("categories_keywords",
                                                   explode(col("categories_keywords"))).selectExpr("sentence_id",
                                                                                                   "categories_keywords.category AS category",
                                                                                                   "categories_keywords.keyword AS keyword")
    logger.info(
        f"Write post static and comment static to {mode}{bucket}/{output_dir}/post_static and {mode}{bucket}/{output_dir}/comment_static")
    post_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/post_static")
    comment_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/comment_static")
    logger.info(
        f"Post and Comment Static Write Complete. {mode}{bucket}/{output_dir}/post_static, {mode}{bucket}/{output_dir}/comment_static")
    sentence_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/sentence")
    logger.info(f"Sentence Table Write Complete. {mode}{bucket}/{output_dir}/sentence")
    keyword_category.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/keyword_category")
    logger.info(f"Sentence Processed Table Write Complete. {mode}{bucket}/{output_dir}/keyword_category")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--input_post_paths', nargs='+', help='Input parquet file paths')
    parser.add_argument('--input_comment_paths', nargs='+', help='Input parquet file paths')
    parser.add_argument('--output_dir', help='Output path')
    args = parser.parse_args()
    mode = "s3://"
    bucket = args.bucket
    input_post_paths = args.input_post_paths
    input_comment_paths = args.input_comment_paths
    output_dir = args.output_dir
    s3_input_post_paths = [f"{mode}{bucket}/{input_post_path}" for input_post_path in input_post_paths]
    s3_input_comment_paths = [f"{mode}{bucket}/{input_comment_path}" for input_comment_path in input_comment_paths]
    spark = SparkSession.builder.appName("vroomcast-spliter-job").getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("vroomcast-spliter-job") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    #     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    #     .getOrCreate()
    print(spark.sparkContext.uiWebUrl)
    post_df = spark.read.parquet(*s3_input_post_paths)
    comment_df = spark.read.parquet(*s3_input_comment_paths)
    logger.info(f"Post and Comment Read Complete.")
    transform_static_data(post_df, comment_df)
    spark.stop()

