## EC2 t3.large 인스턴스에 Apache Superset 설치하는 과정

**Docker 설치 후 →**
```bash
git clone https://github.com/apache/superset.git
```
```bash
cd /superset
```
```bash
docker-compose -f docker-compose-non-dev.yml pull
```
```bash
docker-compose -f docker-compose-non-dev.yml up
```
#### ⚠️ 절대 Superset의 모든 계정의 `is Active?` 값이 `False`가 되면 **안됩니다**. ⚠️
#### 만약 그런 상황에 처한다면?...
`export FLASK_APP=superset`
`superset fab create-user` 
이후 새로 계정 생성. (이 때, id, pw 모두 기존 계정과 겹치면 안됩니다.)

## Redshift와 EC2의 Superset, 로컬 컴퓨터를 연결하는 과정
- RedShift 보안 그룹 5439 포트 인바운드 규칙에 Superset이 설치된 EC2의 보안 그룹 연결
- EC2는 22, 8088 포트 인바운드를 로컬 컴퓨터와 연결
- `EC2_public_IP:8088`로 Superset 접속
- Superset에서 Redshift 계정을 연결