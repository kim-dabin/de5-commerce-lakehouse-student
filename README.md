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

그 다음 해당 차시 폴더로 이동합니다.

```bash
cd sessions/1차시
```

각 차시 폴더 안의 `DOWNLOAD_GUIDE.md`를 먼저 읽고, 그 폴더 안에서 명령을 실행합니다.

## 차시별 폴더

| 차시 | 폴더 | 주제 |
|---:|---|---|
| 1차시 | `sessions/1차시` | 전체 아키텍처 청사진 |
| 2차시 | `sessions/2차시` | 로컬 Docker 실습 환경 |
| 3차시 | `sessions/3차시` | Kafka 이벤트 수집 |
| 4차시 | `sessions/4차시` | Flink와 Paimon Bronze |
| 5차시 | `sessions/5차시` | Spark와 Iceberg Batch BI |
| 6차시 | `sessions/6차시` | Airflow 오케스트레이션 |
| 7차시 | `sessions/7차시` | StarRocks Realtime OLAP와 BI |
| 8차시 | `sessions/8차시` | 최종 포트폴리오 정리 |

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
