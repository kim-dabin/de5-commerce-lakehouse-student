# 07차시 자료: StarRocks Realtime OLAP와 BI

이 폴더는 데이터 엔지니어 부트캠프 5기 B주제 라이브 스터디의 07차시 수강생 실습 자료입니다.

## 이번 차시 범위

Kafka 이벤트를 StarRocks realtime serving layer로 적재하고, Streamlit BI에서 realtime 지표와 batch 지표를 비교합니다.

## 먼저 확인할 것

1. Docker Desktop을 실행합니다.
2. 터미널에서 압축을 푼 폴더로 이동합니다.
3. 처음 실행할 때는 아래 공통 명령을 먼저 실행합니다.

```bash
cp .env.example .env
./scripts/check-env.sh
```

## 대표 실행 명령

```bash
docker compose -f docker-compose.lite.yml up -d --build
./scripts/reset-kafka-topic.sh
./scripts/produce-kafka.sh
./scripts/start-realtime-olap.sh
./scripts/reset-realtime-olap.sh
./scripts/load-realtime-olap-from-kafka.sh
./scripts/query-realtime-olap-metrics.sh
./scripts/start-streamlit-bi.sh
```

## 제출/질문 기준

- 과제 제출은 디스코드의 해당 차시 채널에 올립니다.
- 실행이 실패하면 전체 화면보다 터미널의 에러 메시지와 실행한 명령어를 함께 캡처합니다.
- 로컬 PC 성능에 따라 첫 Docker 이미지 다운로드와 Spark 패키지 다운로드는 시간이 걸릴 수 있습니다.

## 포함하지 않은 것

- 멘토 운영 문서, 출석/녹화 관리 문서, 내부 검토 자료는 수강생 배포 자료에 포함하지 않습니다.
- 참고 답안은 기본적으로 포함하지 않습니다. 멘토가 수업 이후 공유할 때만 `INCLUDE_SOLUTIONS=1`로 패키지를 다시 생성합니다.
