# DE 2팀 Vroomcast 저장소입니다.

## 프로젝트 개요

* 현대 자동차에서 신규 모델이 출시되었을 때 커뮤니티 사이트에서 해당 모델에 대한 소비자들의 반응을 모니터링합니다.
* 특정 이슈에 대한 대화가 얼마나 이루어지고 있는지를 시각화하고 알림을 제공합니다.

### 주요 기능
* 30분 마다 커뮤니티 게시글 및 댓글 모니터링
* 이슈별 대화량 측정
* 중요 이슈 발생 시 즉각적인 알림 제공
* 데이터 및 집계 결과 시각화

각 모듈의 기능에 대한 설명은 아래 디렉토리를 참고하세요.

## 디렉터리 구조

* [.github/workflows/](.github/workflows) : Github Actions를 이용한 CI/CD
* [combine/](combine) : S3에 적재된 크롤링된 데이터를 병합
* [extract/bobaedream/](extract/bobaedream) : 보배드림 크롤러  
* [extract/clien/](extract/clien) : 클리앙 크롤러
* [extract/dcinside/](extract/dcinside) : 디시인사이드 크롤러
* [mwaa/](mwaa) : 데이터 파이프라인 제어를 위한 Airflow 관련 파일
* [notification/](notification) : AWS Lambda 기반 Redshift 트렌드 분석 및 Slack 알림 시스템
* [transform/](transform) : AWS Lambda 기반 데이터 변환 및 감성 분석
* [visualize/](visualize) : Apache Superset 설치를 위한 가이드

## 참고 자료

* [유튜브 시연 영상](https://youtu.be/T4YReov0K7w)
* [Notion](https://striped-wineberry-0a1.notion.site/1904f557972a8000b47bf42771d734e9)

## 아키텍처

![Image](https://github.com/user-attachments/assets/a66f423b-1965-40a0-b149-cb2d4b69ae3c)

## 팀원 소개

<table width="100%">
<tbody><tr>
    <td width="33.33%" align="center"><b>공도한</b></td>
    <td width="33.33%" align="center"><b>박산야</b></td>
    <td width="33.33%" align="center"><b>최민제</b></td>
</tr>
<tr>
    <td align="center"><a href="https://github.com/reudekx"><img src="https://github.com/reudekx.png" width="100" height="100" style="max-width: 100%;"></a></td>
    <td align="center"><a href="https://github.com/sanyapark"><img src="https://github.com/sanyapark.png" width="100" height="100" style="max-width: 100%;"></a></td>
    <td align="center"><a href="https://github.com/minjacho42"><img src="https://github.com/minjacho42.png" width="100" height="100" style="max-width: 100%;"></a></td>
</tr>
<tr>
    <td align="center"><a href="https://github.com/reudekx">@reudekx</a></td>
    <td align="center"><a href="https://github.com/sanyapark">@sanyapark</a></td>
    <td align="center"><a href="https://github.com/minjacho42">@minjacho42</a></td>
</tr>
<tr>
    <td align="center">DE</td>
    <td align="center">DE</td>
    <td align="center">DE</td>
</tr>
</tbody></table>
