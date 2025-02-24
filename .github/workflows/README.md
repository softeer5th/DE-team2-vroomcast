# GitHub Actions 워크플로우 설명

## 개요
이 저장소는 AWS Lambda 및 Spark 기반 데이터 파이프라인을 자동화하는 GitHub Actions 워크플로우를 포함하고 있습니다. 
각 워크플로우는 특정 데이터 소스에서 정보를 추출하거나 변환하는 역할을 수행하며, 안정적이고 효율적인 데이터 처리를 지원합니다.

---

## 워크플로우 설명

### 1. 게시판 데이터 추출 (Bobaedream, Clien, DCinside)
#### `bobaedream-extract-deploy.yml`, `clien-extract-deploy.yml`, `dcinside-extract-deploy.yml`
- 특정 게시판(Bobaedream, Clien, DCinside)에서 데이터를 크롤링하는 AWS Lambda를 배포하는 워크플로우
- `main` 브랜치에서 관련 파일 변경 발생 시 또는 `workflow_dispatch` 이벤트를 통해 실행
- AWS ECR에 Docker 이미지를 빌드 및 푸시한 후 Lambda 함수를 생성하거나 업데이트
- 배포된 Lambda의 상태를 확인하여 정상 작동 여부 검증

#### `bobaedream-extract-test.yml`, `clien-extract-test.yml`, `dcinside-extract-test.yml`
- 크롤링 Lambda의 테스트 환경을 배포하고 실행하는 워크플로우
- `dev` 브랜치에서 관련 파일 변경 시 또는 `workflow_dispatch` 이벤트를 통해 실행
- 테스트용 Docker 이미지를 빌드 및 푸시한 후, 테스트용 Lambda 함수를 생성
- Lambda 실행 결과를 확인하고 응답 상태 검증

### 2. 감성 분석 (Sentiment Analysis)
#### `transform-sentiment-deploy.yml`
- 수집된 데이터를 감성 분석(Sentiment Analysis)하는 AWS Lambda를 배포하는 워크플로우
- `main` 브랜치에서 `transform/sentiment/**` 파일 변경 발생 시 또는 `workflow_dispatch` 이벤트를 통해 실행
- 감성 분석 Lambda의 Docker 이미지를 빌드하고 AWS ECR에 푸시
- OpenAI API 키를 환경 변수로 설정하여 Lambda에서 감성 분석 수행 가능하도록 설정

#### `transform-sentiment-test.yml`
- 감성 분석 Lambda의 테스트 환경을 배포하고 실행하는 워크플로우
- `dev` 브랜치에서 `transform/sentiment/**` 파일 변경 발생 시 또는 `workflow_dispatch` 이벤트를 통해 실행
- 테스트용 Lambda 이미지 빌드 및 푸시, Lambda 함수 생성 후 실행 결과 검증

### 3. Spark 데이터 처리
#### `transform-spark-file-upload.yml`
- Spark 작업을 AWS EMR에서 실행하기 위해 필요한 코드를 S3에 업로드하는 워크플로우
- `transform/emr` 브랜치에서 `transform/main/**` 파일 변경 발생 시 또는 `workflow_call` 이벤트를 통해 실행
- `transform/main/` 디렉터리 내 파일을 AWS S3로 업로드하여 EMR 작업 실행 준비

---

## 실행 방법
GitHub Actions에서 특정 조건을 만족하면 자동 실행됩니다. 
수동 실행이 필요한 경우 GitHub `Actions` 탭에서 원하는 워크플로우를 선택하고 `Run workflow` 버튼을 클릭하면 됩니다.

