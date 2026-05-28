# 00 환경 점검

수업 전 로컬 PC 상태를 확인하는 차시 공통 체크리스트입니다.

## 확인할 것

- Docker Desktop이 설치되어 있는지
- Docker daemon이 실행 중인지
- Docker Compose v2를 사용할 수 있는지
- host RAM과 Docker Desktop memory 설정이 실습에 충분한지

## 실행

```bash
cp .env.example .env
./scripts/check-env.sh
```

정상이라면 Docker version, Docker Compose version, Docker daemon 상태, host memory 정보가 출력됩니다.

## 기준

- 권장 host RAM: 16GB 이상
- 권장 Docker Desktop memory: 10~12GB
- OpenMetadata까지 함께 실행하려면 Docker memory 12GB 이상이 사실상 필요합니다.

## 수업 중 사용할 질문

- Docker는 설치되어 있지만 daemon이 꺼져 있는 상태와 아예 설치되지 않은 상태는 어떻게 구분할 수 있을까요?
- host RAM과 Docker Desktop memory limit은 왜 따로 봐야 할까요?
- 수업 실습에서 OpenMetadata를 필수로 넣지 않은 이유는 무엇일까요?
