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

그 다음 해당 차시 폴더로 이동합니다. 예를 들어 5차시 수업 전에는 아래처럼 이동합니다.

```bash
cd sessions/05-project-mvp
```

각 차시 폴더 안의 `DOWNLOAD_GUIDE.md`를 먼저 읽고, 그 폴더 안에서 명령을 실행합니다.

## 현재 공개된 차시

| 차시 | 폴더 | 주제 |
|---:|---|---|
| 1차시 | `sessions/01-architecture-blueprint` | 전체 아키텍처 청사진 |
| 2차시 | `sessions/02-local-docker-env` | 로컬 Docker 실습 환경 |
| 3차시 | `sessions/03-kafka-producer` | Kafka UX 이벤트 수집 |
| 4차시 | `sessions/04-flink-paimon` | Flink와 Paimon Bronze |
| 5차시 | `sessions/05-project-mvp` | 프로젝트 MVP 완성 |
| 6차시 | `sessions/06-ops-recovery-drills` | 운영 장애 대응 / 복구 드릴 |
| 7차시 | `sessions/07-quality-serving` | 데이터 품질과 StarRocks Serving |

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

## 참고 리소스

| 리소스 | 경로 | 용도 |
|---|---|---|
| OpenMetadata 리니지 예시 | `resources/openmetadata-lineage/README.md` | 데이터 카탈로그/리니지 관점으로 파이프라인을 읽는 참고 자료 |
