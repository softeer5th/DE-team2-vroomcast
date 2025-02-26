import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from kiwipiepy import Kiwi
import pandas as pd
import os
import argparse
import logging

logger = logging.getLogger(__name__)

# Pandas UDF의 반환 타입을 StructType으로 지정
keyword_dict = {
  "design": [
    "디자인","실내","실외","내장","외장","전면부","후면부","루프라인","시트",
    "재질","범퍼","형상","크롬","몰딩","투톤","컬러","대시보드","센터페시아","계기판",
    "핸들","암레스트","도어트림","내장재","마감","상태","헤드램프","테일램프","안개등","LED",
    "라이트","HID","DRL","라디에이터","그릴","사이드미러","루프","스포일러","리어","사이드",
    "스커트","익스테리어","인테리어","페이스리프트","풀체인지","패밀리룩","고급감","라인","배색","우드트림",
    "스티어링휠","가죽","패턴","나파","착좌감","공간감","파노라마","선루프","썬루프","무드등",
    "완성도","측면","실루엣","바디킷","에어댐","공기역학","루프랙","하이글로시","바디","대형",
    "프로젝터","램프","우아","와이드","헤드업","디스플레이","버킷","앰비언트","에어벤트","조명",
    "인조가죽","메탈","트림","스티치","포인트","블랙베젤","카본","하차감","스포티",
    "다이내믹","가니쉬","수평","기조","휠베이스","박스","스타일","슬림","스플리터"
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
    "개소세","부가세","고정비","변동비","할부","리스","렌트","중고","시세",
    "감가","잔존","가치","보증","연장","기간","할인이벤트","프로모션","특판","출고가",
    "실구매가","계약금","잔금","월납입금","잔가","영맨","할인","파이낸스","이자","금리",
    "고정","변동","무이자","포인트","캐시백","보조금","수소차","폐차","리세일",
    "매도","매입","딜러","매물","호갱","깎기","가격협상","옵션가","악옵","옵션질",
    "유료옵션","결제","카드","현금","견적","사전","계약","출고","재고","무옵션",
    "할부금","인수","거부","잔가보장","잔가형","리스","부담금","중고보상","업그레이드","다운그레이드",
    "세이브","구입비","초기","유지","극할인","대차","반납","취등록세","견적서"
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

@pandas_udf(ArrayType(
    StructType([
        StructField("id", StringType(), False),
        StructField("sentence", StringType(), True),
        StructField("from_post", BooleanType(), True),
        StructField("categories_keywords", ArrayType(  # 카테고리 키워드 리스트
            StructType([
                StructField("category", StringType(), True),
                StructField("keyword", StringType(), True)
            ])
        ), True)
    ])
))
def get_sentences(source_ids: pd.Series, titles: pd.Series, contents: pd.Series) -> pd.Series:
    """
    내용을 문장 단위로 요약하고, 각 문장에 대해 키워드와 해당 키워드의 카테고리를 주석으로 달아
    Spark DataFrame에서 처리할 수 있도록 구조화된 배열을 반환합니다.

    Parameters:
        source_ids : pd.Series
            문장에 고유한 ID를 할당하기 위해 사용되는 소스 식별자를 포함하는 pandas Series입니다.
        titles : pd.Series
            분석할 콘텐츠의 제목을 포함하는 pandas Series로, 각 제목은 문장 단위로 분할됩니다.
        contents : pd.Series
            분석할 콘텐츠의 본문을 포함하는 pandas Series로, 각 본문은 문장 단위로 분할된 후,
            키워드 및 카테고리와 함께 주석이 달립니다.

    Returns:
        pd.Series
            구조화된 문장 목록을 포함하는 pandas Series를 반환합니다. 각 문장은 다음 필드를 포함하는
            구조화된 레코드로 표현됩니다:
            - `id`: 고유 식별자
            - `sentence`: 문장의 텍스트
            - `from_post`: 해당 문장이 제목에서 왔는지 여부를 나타내는 불리언 값
            - `categories_keywords`: 문장에서 감지된 키워드 및 카테고리 목록
    """
    kiwi = Kiwi()
    results = []
    for source_id, title, content in zip(source_ids, titles, contents):
        result_per_content = []
        sentences = []
        if title:
            for kiwi_sent in kiwi.split_into_sents(title):
                sentences.append(kiwi_sent.text)
        if content:
            for kiwi_sent in kiwi.split_into_sents(content):
                sentences.append(kiwi_sent.text)
        for idx, sentence in enumerate(sentences):
            upper_sentence = sentence.upper()  # 대문자로 변환하여 키워드 검색
            categories_keywords = []  # 각 문장의 키워드 결과 저장
            if len(sentence) < 6:
                continue
            # 카테고리와 키워드 탐지
            for category, keywords in keyword_dict.items():
                for keyword in keywords:
                    if keyword in upper_sentence:
                        # 키워드 탐지 시 카테고리와 키워드를 구조체로 추가
                        categories_keywords.append({"category": category, "keyword":keyword})
            result_per_content.append((f"{source_id}_{idx}", sentence, True if title else False, categories_keywords))
        results.append(result_per_content)
    return pd.Series(results)

# 링크 필터링, 유저 필터링
def regex_replace_privacy(df):
    """
    DataFrame의 "content" 열에서 민감한 정보 또는 원하지 않는 패턴을 치환합니다.

    세부 변환 사항:
    1. URL을 "<URL>"로 대체하여 제거하거나 치환합니다.
    2. 사용자 핸들(@username 등)을 "<USER>"로 익명화합니다.
    3. emaildmf "<EMAIL>"로 익명화합니다.
    4. &nbsp;와 같은 불필요한 공백 문자 및 관련 문자를 정리합니다.
    5. 연속된 여러 개의 공백을 단일 공백으로 변환합니다.
    6. 문장 끝 문장부호(마침표, 느낌표 등) 뒤에 공백이 없는 경우 공백을 추가합니다.

    Parameters:
        df (DataFrame): 정규식 변환을 적용하여 정리할 텍스트 데이터가 포함된 "content" 열을 가진 PySpark DataFrame입니다.

    Returns:
        DataFrame: 변환이 적용된 PySpark DataFrame으로, "content" 열이 정리된 상태로 반환됩니다.
    """
    df = df.withColumn("content", regexp_replace(col("content"), r'https?://\S+', '<URL>'))
    df = df.withColumn("content",
                   regexp_replace(col("content"), r'(?<=\s|^)@[A-Za-z0-9_가-힣\-]+', '<USER>'))
    df = df.withColumn("content", regexp_replace(col("content"),
                       r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', '<EMAIL>'))
    df = df.withColumn("content",
                       regexp_replace(col("content"), r'&nbsp;|\xa0', ''))
    df = df.withColumn("content", trim(col("content")))
    df = df.withColumn("content", regexp_replace(col("content"), r'\s+', ' '))
    df = df.withColumn("content", regexp_replace(col("content"), r"([가-힣]+[.?!])(?=[가-힣0-9a-zA-Z])", r"$1 "))
    df = df.withColumn("content", regexp_replace(col("content"), r'(ㅋ){4,}', 'ㅋㅋㅋㅋ'))
    return df

def split_content_to_sentences(df):
    """
    DataFrame의 내용을 개별 문장으로 분할하여 처리합니다.
    문장이 분할된 후, ID, 카테고리, 키워드 등의 메타데이터와 함께
    구조화된 형태로 반환됩니다.

    Parameters:
        df (DataFrame): "id", "title", "content", "created_at" 등의 열을 포함한 Spark DataFrame입니다.
            "title" 열은 존재할 수도 있고 존재하지 않을 수도 있습니다.

    Returns:
        DataFrame: 원본 DataFrame에서 추출한 문장과 관련 메타데이터가 포함된 Spark DataFrame을 반환합니다.
        반환되는 DataFrame의 열은 다음과 같습니다:
            - id: 문장의 고유 식별자
            - source_id: 입력 DataFrame에서의 원본 식별자
            - from_post: 해당 문장이 게시글(타이틀)에서 추출되었는지를 나타내는 불리언 값 또는 플래그
            - sentence: 추출된 문장 텍스트
            - created_at: 원본 DataFrame에서 가져온 타임스탬프
            - categories_keywords: 문장에서 추출된 카테고리 및 키워드 목록
    """
    df = df.withColumn(
        "sentences",
        explode(
            get_sentences(
                col("source_id"), col("title"), col("content")
            )
        )
    ).selectExpr("sentences.id AS id", "source_id", "sentences.from_post AS from_post",
                 "sentences.sentence AS sentence", "created_at",
                 "sentences.categories_keywords AS categories_keywords"
    )
    return df


def transform_static_data(post_df: DataFrame, comment_df: DataFrame, optimize_skew_len: int) -> None:
    """
    게시글 및 댓글 데이터를 변환하여 개인정보를 제거하고 문장 단위로 분할한 후 저장하는 함수.

    이 함수는 다음 단계를 수행합니다:
    1. **개인정보 보호 전처리**: 정규식을 이용해 게시글 및 댓글에서 민감한 정보를 제거합니다.
    2. **데이터 병합**: 게시글과 댓글 데이터를 하나의 DataFrame으로 합칩니다.
    3. **(선택) 데이터 스큐 최적화**: `optimize_skew`가 0이 아닌 경우, `explode()` 실행 전 데이터를 균등하게 분배하여 스큐 현상을 방지합니다.
    4. **문장 분할**: Pandas UDF(`get_sentences`)를 사용하여 본문을 문장 단위로 변환합니다.
    5. **캐싱**: 문장 데이터를 캐싱하여 이후 연산 성능을 최적화합니다.
    6. **데이터 저장**: 변환된 데이터를 Parquet 포맷으로 저장합니다.

    Parameters:
        post_df (DataFrame | None): 게시글 데이터를 포함하는 DataFrame입니다.
            제공될 경우, 정규식 기반의 민감한 정보 치환 및 문장 분할을 수행합니다.
        comment_df (DataFrame | None): 댓글 데이터를 포함하는 DataFrame입니다.
            제공될 경우, 정규식 기반의 민감한 정보 치환 및 문장 분할을 수행합니다.
        optimize_skew_len (int): 데이터 스큐 최적화 적용을 위한 파티션당 글자 길이.

    Returns:
        None
    """
    pre_post_df = None
    pre_comment_df = None
    if post_df is not None: # post_df 가 존재하면, regex를 통해서 개인정보와 이후 transform을 위한 전처리 진행
        post_df = regex_replace_privacy(post_df)
        pre_post_df = post_df.selectExpr("id AS source_id", "title", "content", "created_at")
    if comment_df is not None: # comment_df 가 존재하면, regex를 통해서 개인정보와 이후 transform을 위한 전처리 진행
        comment_df = regex_replace_privacy(comment_df)
        pre_comment_df = comment_df.selectExpr("id AS source_id", "CAST(NULL AS STRING) AS title", "content", "created_at")
    if pre_post_df is not None and pre_comment_df is not None:
        pre_sentence_df = pre_post_df.union(pre_comment_df)
    elif pre_post_df is not None:
        pre_sentence_df = pre_post_df
    else:
        pre_sentence_df = pre_comment_df

    # optimize_skew인 경우, explode로 인한 skewing이 일어나기 전에, 미리 데이터를 쪼개놓는다.
    if optimize_skew_len > 0:
        pre_sentence_df = pre_sentence_df.withColumns({
            "sentence_length": length(col("content")),
            "cumulative_length": sum(col("sentence_length")).over(Window.orderBy("source_id")),
        }).withColumn("partition_id", round(col("cumulative_length") / lit(optimize_skew_len)).cast("int"))
        pre_sentence_df = pre_sentence_df.repartitionByRange(col("partition_id"))

    if optimize_skew_len < 0:
        pre_sentence_df.withColumn("salted_id", concat(col("source_id"), lit("_"), F.floor(F.rand() * 10)))
        pre_sentence_df = pre_sentence_df.repartition(col("salted_id"))
        pre_sentence_df = pre_sentence_df.drop("salted_id")

    total_sentence_df = split_content_to_sentences(pre_sentence_df)
    total_sentence_df = total_sentence_df.cache()
    sentence_df = total_sentence_df.selectExpr("id", "source_id", "from_post", "sentence", "created_at")
    keyword_category = total_sentence_df.withColumn(
        "categories_keywords", explode(col("categories_keywords"))
    ).selectExpr(
        "id as sentence_id", "categories_keywords.category AS category", "categories_keywords.keyword AS keyword"
    )

    logger.info(
        f"Write post static and comment static to {mode}{bucket}/{output_dir}/post_static and {mode}{bucket}/{output_dir}/comment_static"
    )
    if post_df is not None:
        post_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/post_static")
    if comment_df is not None:
        comment_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/comment_static")

    logger.info(
        f"Post and Comment Static Write Complete. {mode}{bucket}/{output_dir}/post_static, {mode}{bucket}/{output_dir}/comment_static"
    )
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
    parser.add_argument('--optimize_skew_len', default=0, type=int, help='Enable skew optimization')
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
    # print(spark.sparkContext.uiWebUrl)
    try:
        post_df = spark.read.parquet(*s3_input_post_paths)
    except AnalysisException as e:
        logger.warning(f"Post Read Error: {e}")
        post_df = None
    try:
        comment_df = spark.read.parquet(*s3_input_comment_paths)
    except AnalysisException as e:
        logger.warning(f"Comment Read Error: {e}")
        comment_df = None
    logger.info(f"Post and Comment Read Complete.")
    if post_df is None and comment_df is None:
        logger.error("There are not valid post and comment data.")
        exit(0)
    transform_static_data(post_df, comment_df, optimize_skew_len=args.optimize_skew_len)
    spark.stop()