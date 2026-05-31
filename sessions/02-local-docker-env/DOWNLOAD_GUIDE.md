# 2차시 다운로드/실행 가이드

## GitHub로 받은 경우

```bash
git clone https://github.com/kim-dabin/de5-commerce-lakehouse-student.git
cd de5-commerce-lakehouse-student
git pull
cd sessions/02-local-docker-env
```

## 처음 실행 전 공통 확인

Windows 사용자는 WSL2 Ubuntu 또는 Git Bash에서 실행해주세요.

```bash
cp .env.example .env
./scripts/check-env.sh
```

## 2차시 시작 전 권장 작업

가능한 분은 수업 전에 Docker 이미지 빌드까지 미리 진행해주세요. 첫 실행은 PC와 네트워크에 따라 30분 이상 걸릴 수 있습니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
./scripts/produce-kafka.sh --max-events 10 --rate-per-second 2
MAX_MESSAGES=5 ./scripts/consume-kafka.sh
```

## 이번 차시 대표 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/smoke-test.sh
```

실행이 실패하면 실행한 명령어와 터미널 에러 메시지를 함께 캡처해 디스코드 해당 차시 채널에 올려주세요.

첫 Docker 이미지 다운로드/빌드는 PC와 네트워크에 따라 30분 이상 걸릴 수 있습니다.
