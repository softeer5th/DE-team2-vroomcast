# Airflow

## 개요

* Airflow를 사용하여 데이터를 추출, 처리 및 적재하는 파이프라인을 제어합니다.
* Docker를 통해 로컬 환경에서 개발 및 테스트를 진행한 후 Amazon  MWAA에 배포됩니다.

## 주요 디렉터리 구조

```
📦bucket
 ┣ 📂dags
 ┃ ┣ 📂configs			# 설정 관련
 ┃ ┣ 📂modules			# 각종 Task 및 상수 정의
 ┃ ┣ 📂test				# 테스트용 DAG
 ┃ ┣ 📂utils			# 유틸리티
 ┃ ┗ 📜workflow.py		# 메인 DAG
 ┣ 📜requirements.txt	# 패키지 목록(비어 있음)
 ┗ 📜startup.sh			# 시작 시 실행 스크립트(비어 있음)
```

## 메인 DAG

* [bucket/dags/workflow.py](bucket/dags/workflow.py)

![Image](https://github.com/user-attachments/assets/87b9746a-f567-4af9-bc0a-f5c717896025)

## 작업 흐름

#### 1. DAG 실행 중 공유하기 위한 시간 정보를 구해 저장합니다.

![Image](https://github.com/user-attachments/assets/3409d0db-af00-428d-b14e-780bae427af4)

#### 2. Lambda를 호출하여 게시글과 댓글을 추출한 뒤 Parquet로 병합하여 S3에 적재합니다.

* 추출 결과를 집계해 Slack으로 알림을 전송합니다.
* 각 추출 작업이 실패하더라도 다음 작업이 수행됩니다.

![Image](https://github.com/user-attachments/assets/7364e1de-a5e8-4f95-bd63-bdb4fc61ca59)

#### 3. Spark Job 코드를 EMR 클러스터에 제출하여 데이터 변환을 수행하고 Lambda를 호출하여 문장별 감성 분석을 진행합니다.

![Image](https://github.com/user-attachments/assets/e24fd0e0-b72a-4e43-845b-95cf1c282d82)

#### 4. S3에 적재된 추출 데이터를 Redshift로 적재합니다.

* 동시에 많은 작업이 실행되는 것을 방지하고자 순서를 부여하였습니다.
* 각 작업이 실패하더라도 다음 작업이 계속 수행됩니다.

![Image](https://github.com/user-attachments/assets/b8216231-167f-4305-a495-6183ab04ccba)

#### 5. Redshift에 쿼리를 실행하여 특정 지표에 대한 높은 변화량을 감지하면 알림을 발송하는 Lambda를 호출한 후 파이프라인 종료 알림을 발송합니다.

![Image](https://github.com/user-attachments/assets/f247e23c-c03b-4203-be26-27e0989a0689)

## 실행 방법

`docker-compose.yml`이 위치한 디렉터리에서 다음 명령어를 입력하세요.

```bash
docker compose up -d
```

이후 웹 브라우저를 통해 `localhost:8080`에 접속한 뒤 UI를 확인하세요. 	