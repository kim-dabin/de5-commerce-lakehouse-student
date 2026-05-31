# DE5 커머스 Lakehouse 학생 실습 자료

데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 학생용 실습 저장소입니다.

각 차시는 `sessions/` 아래에 자기완결형 폴더로 들어 있습니다. 수업 시작 전에는 `git pull`로 최신 자료를 받은 뒤 해당 차시 폴더에서 실습합니다.

## 처음 한 번만 실행

```bash
git clone https://github.com/kim-dabin/de5-commerce-lakehouse-student.git
cd de5-commerce-lakehouse-student
```

## 매 차시 시작 전

repo root 또는 차시 폴더 안에서 아래 명령을 실행합니다.

```bash
git pull
```

그 다음 해당 차시 폴더로 이동합니다. 예를 들어 2차시 수업 전에는 아래처럼 이동합니다.

```bash
cd sessions/02-local-docker-env
```

각 차시 폴더 안의 `DOWNLOAD_GUIDE.md`를 먼저 읽고, 그 폴더 안에서 명령을 실행합니다.

## 현재 공개된 차시

| 차시 | 폴더 | 주제 |
|---:|---|---|
| 1차시 | `sessions/1차시` | 전체 아키텍처 청사진 |
| 2차시 | `sessions/02-local-docker-env` | 로컬 Docker 실습 환경 |

## 현재 해야 할 일

- 1차시 과제 확인: `sessions/1차시/ASSIGNMENT.md`
- 2차시 사전 준비: `sessions/02-local-docker-env/DOWNLOAD_GUIDE.md`
- 2차시 시작 전 가능하면 Docker 이미지 빌드까지 미리 진행

## 실습 전 공통 확인

각 차시 폴더에서 아래 명령으로 환경을 확인합니다.

```bash
cp .env.example .env
./scripts/check-env.sh
```

Docker Desktop을 먼저 실행해야 합니다.

## 주의

- 각 차시 폴더는 독립 실행이 가능하도록 필요한 파일을 함께 포함합니다.
- 이전 차시 폴더에서 Docker stack을 실행 중이라면 다음 차시로 이동하기 전에 `docker compose -f docker-compose.lite.yml down`으로 중지하는 것을 권장합니다.
- `.env`, Python 가상환경, Docker volume, 실행 결과물은 Git에 올리지 않습니다.
- 과제 제출은 디스코드의 해당 차시 채널을 사용합니다.
