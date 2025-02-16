#!/bin/bash
set -e  # 에러 발생 시 즉시 종료

# 필수 패키지 설치
sudo yum install -y python3-devel

# 최신 wheel 설치
pip3 install --upgrade wheel

# KSS 설치 (캐시 없이)
pip3 install --no-cache-dir kss