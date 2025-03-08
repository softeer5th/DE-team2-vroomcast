# AWS Lambda 기반 Redshift 트렌드 분석 및 Slack 알림 시스템

AWS Lambda에서 실행되며, Redshift에서 데이터를 가져와 트렌드를 분석하고, 이상 징후를 감지하면 Slack으로 알림을 보내는 기능을 수행합니다.

## 주요 기능
- **Redshift에서 트렌드 데이터 추출**
- **중복 제거 및 테이블 갱신**
- **트렌드 변화량 분석 및 이상 탐지**
- **Slack 알림 전송**

## 환경 변수 설정
이 코드를 실행하기 위해 아래 환경 변수를 설정해야 합니다:
- `SECRET_ID`: Redshift 접속을 위한 AWS Secrets Manager의 ID
- `SLACK_WEBHOOK_URL`: Slack 알림을 전송할 웹훅 URL
- `SUPERSET_URL`: 분석 대시보드를 확인할 수 있는 Superset URL

