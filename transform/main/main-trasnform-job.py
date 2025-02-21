from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kiwipiepy import Kiwi
import pandas as pd
import os
import argparse
import logging

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
    df = df.withColumn("content", regexp_replace(col("content"), r'https?://\S+', ''))
    df = df.withColumn("content", regexp_replace(col("content"), r'@[\w\uac00-\ud7af]+', ''))
    return df

def transform_static_data(post_df, comment_df):
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
                 "sentences.sentence AS sentence", "created_at", "sentences.categories_keywords AS categories_keywords")
    comment_sentence_df = comment_pre_sentence_df.withColumn(
        "sentences",
        explode(
            get_sentences(
                col("source_id"), col("title"), col("content")
            )
        )
    ).selectExpr("sentences.id AS id", "source_id", "sentences.from_post AS from_post",
                 "sentences.sentence AS sentence", "created_at", "sentences.categories_keywords AS categories_keywords")
    total_sentence_df = post_sentence_df.union(comment_sentence_df)
    total_sentence_df = total_sentence_df.repartition(10)
    total_sentence_df = total_sentence_df.cache()
    sentence_df = total_sentence_df.selectExpr("id", "source_id", "from_post", "sentence", "created_at")
    keyword_category = total_sentence_df.withColumn(
        "categories_keywords", explode(col("categories_keywords"))
    ).selectExpr(
        "id as sentence_id", "categories_keywords.category AS category", "categories_keywords.keyword AS keyword"
    )
    logger.info(
        f"Write post static and comment static to {mode}{bucket}/{output_dir}/post_static and {mode}{bucket}/{output_dir}/comment_static")
    post_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/post_static")
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
    post_df = spark.read.parquet(*s3_input_post_paths)
    comment_df = spark.read.parquet(*s3_input_comment_paths)
    logger.info(f"Post and Comment Read Complete.")
    transform_static_data(post_df, comment_df)
    spark.stop()