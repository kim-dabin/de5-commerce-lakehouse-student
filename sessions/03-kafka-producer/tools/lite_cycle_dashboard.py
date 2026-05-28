#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_HTML = ROOT / "docs" / "lite-cycle-dashboard.html"
MAX_OUTPUT_CHARS = 120_000
BI_JSON_PREFIX = "BI_METRICS_JSON="
BI_QUERY_TIMEOUT_SECONDS = 180


def cmd(*parts: str) -> list[str]:
    return list(parts)


STEPS: dict[str, dict[str, Any]] = {
    "check_env": {
        "title": "환경 점검",
        "layer": "Setup",
        "command": cmd("./scripts/check-env.sh"),
        "goal": "Docker CLI와 Docker Desktop 실행 상태를 확인한다.",
        "success": "Environment check completed",
    },
    "compose_up": {
        "title": "실습 스택 실행",
        "layer": "Setup",
        "command": cmd("docker", "compose", "-f", "docker-compose.lite.yml", "up", "-d", "--build"),
        "goal": "Kafka, MinIO, Iceberg REST, Flink, Spark 컨테이너를 실행한다.",
        "success": "컨테이너들이 Up 상태",
    },
    "smoke_test": {
        "title": "스택 점검",
        "layer": "Setup",
        "command": cmd("./scripts/smoke-test.sh"),
        "goal": "Kafka 토픽, MinIO 버킷, Iceberg REST endpoint를 빠르게 확인한다.",
        "success": "Smoke test completed",
    },
    "reset_kafka": {
        "title": "Kafka 토픽 초기화",
        "layer": "Kafka",
        "command": cmd("./scripts/reset-kafka-topic.sh"),
        "goal": "commerce-events 토픽을 삭제 후 다시 만든다.",
        "success": "commerce-events 토픽 describe 출력",
    },
    "produce_kafka": {
        "title": "샘플 이벤트 적재",
        "layer": "Kafka",
        "command": cmd("./scripts/produce-kafka.sh"),
        "goal": "커머스 행동 이벤트 샘플 JSONL을 Kafka에 넣는다.",
        "success": "sent=240",
    },
    "consume_kafka": {
        "title": "Kafka 메시지 확인",
        "layer": "Kafka",
        "command": cmd("./scripts/consume-kafka.sh"),
        "goal": "토픽에 들어간 메시지를 눈으로 확인한다.",
        "success": "JSON 이벤트 출력",
    },
    "reset_paimon": {
        "title": "Paimon Bronze 초기화",
        "layer": "Flink/Paimon",
        "command": cmd("./scripts/reset-paimon-bronze.sh"),
        "goal": "이전 Bronze 테이블을 삭제한다.",
        "success": "에러 없이 종료",
    },
    "run_flink": {
        "title": "Flink -> Paimon 적재",
        "layer": "Flink/Paimon",
        "command": cmd("./scripts/run-flink-paimon-bronze.sh"),
        "goal": "Flink SQL로 Kafka를 읽어 Paimon Bronze에 저장한다.",
        "success": "INSERT 작업 완료",
    },
    "query_paimon": {
        "title": "Paimon Bronze 조회",
        "layer": "Flink/Paimon",
        "command": cmd("./scripts/query-paimon-bronze.sh"),
        "goal": "Bronze row count와 이벤트 타입별 count를 확인한다.",
        "success": "row_count = 240",
    },
    "reset_iceberg": {
        "title": "Iceberg 테이블 초기화",
        "layer": "Spark/Iceberg",
        "command": cmd("./scripts/reset-iceberg-tables.sh"),
        "goal": "이전 Analytics 테이블을 삭제한다.",
        "success": "에러 없이 종료",
    },
    "run_spark": {
        "title": "Spark -> Iceberg 변환",
        "layer": "Spark/Iceberg",
        "command": cmd("./scripts/run-spark-iceberg-transform.sh"),
        "goal": "Paimon Bronze를 읽어 Iceberg 정제/집계 테이블을 만든다.",
        "success": "created=... rows=240",
    },
    "query_iceberg": {
        "title": "Iceberg 결과 조회",
        "layer": "Spark/Iceberg",
        "command": cmd("./scripts/query-iceberg-tables.sh"),
        "goal": "최종 Iceberg 테이블과 집계 결과를 확인한다.",
        "success": "clean_row_count = 240",
    },
    "start_airflow": {
        "title": "Airflow 실행",
        "layer": "Airflow",
        "command": cmd("./scripts/start-airflow.sh"),
        "goal": "Airflow DB, webserver, scheduler를 실행한다.",
        "success": "Airflow UI: http://localhost:8080",
    },
    "list_airflow": {
        "title": "DAG 등록 확인",
        "layer": "Airflow",
        "command": cmd("./scripts/list-airflow-dags.sh"),
        "goal": "de5_lite_lakehouse_pipeline DAG가 등록됐는지 확인한다.",
        "success": "de5_lite_lakehouse_pipeline",
    },
    "trigger_airflow": {
        "title": "DAG 수동 실행",
        "layer": "Airflow",
        "command": cmd("./scripts/trigger-airflow-pipeline.sh"),
        "goal": "Airflow로 전체 파이프라인을 실행한다.",
        "success": "Airflow UI에서 task success",
    },
}


GROUPS: dict[str, dict[str, Any]] = {
    "setup": {
        "title": "환경 준비",
        "summary": "Docker와 기본 스택이 수업을 진행할 수 있는 상태인지 확인한다.",
        "steps": ["check_env", "compose_up", "smoke_test"],
    },
    "kafka": {
        "title": "Kafka 입력",
        "summary": "샘플 이벤트를 Kafka 토픽에 넣고 메시지를 확인한다.",
        "steps": ["reset_kafka", "produce_kafka", "consume_kafka"],
    },
    "flink_paimon": {
        "title": "Flink + Paimon Bronze",
        "summary": "Flink가 Kafka를 읽어 Bronze 저장소에 적재한다.",
        "steps": ["reset_paimon", "run_flink", "query_paimon"],
    },
    "spark_iceberg": {
        "title": "Spark + Iceberg Analytics",
        "summary": "Spark가 Bronze를 읽어 분석용 Iceberg 테이블을 만든다.",
        "steps": ["reset_iceberg", "run_spark", "query_iceberg"],
    },
    "airflow": {
        "title": "Airflow 오케스트레이션",
        "summary": "수동 실행 순서를 DAG 하나로 묶어 실행한다.",
        "steps": ["start_airflow", "list_airflow", "trigger_airflow"],
    },
    "full_cycle": {
        "title": "전체 라이트 사이클",
        "summary": "환경 점검부터 Airflow 실행까지 첫날 데모용 전체 흐름을 수행한다.",
        "steps": [
            "check_env",
            "compose_up",
            "smoke_test",
            "reset_kafka",
            "produce_kafka",
            "consume_kafka",
            "reset_paimon",
            "run_flink",
            "query_paimon",
            "reset_iceberg",
            "run_spark",
            "query_iceberg",
            "start_airflow",
            "list_airflow",
            "trigger_airflow",
        ],
    },
}


runs: dict[str, dict[str, Any]] = {}
current_run_id: str | None = None
lock = threading.Lock()


def public_step(step_id: str) -> dict[str, Any]:
    step = STEPS[step_id]
    return {
        "id": step_id,
        "title": step["title"],
        "layer": step["layer"],
        "command": shlex.join(step["command"]),
        "goal": step["goal"],
        "success": step["success"],
    }


def public_group(group_id: str) -> dict[str, Any]:
    group = GROUPS[group_id]
    return {
        "id": group_id,
        "title": group["title"],
        "summary": group["summary"],
        "steps": [public_step(step_id) for step_id in group["steps"]],
    }


def append_output(run_id: str, text: str) -> None:
    with lock:
        run = runs[run_id]
        run["output"] += text
        if len(run["output"]) > MAX_OUTPUT_CHARS:
            run["output"] = run["output"][-MAX_OUTPUT_CHARS:]
            run["truncated"] = True
        run["updated_at"] = time.time()


def mark_run(run_id: str, status: str, exit_code: int | None = None) -> None:
    global current_run_id
    with lock:
        run = runs[run_id]
        run["status"] = status
        run["exit_code"] = exit_code
        run["finished_at"] = time.time()
        if current_run_id == run_id:
            current_run_id = None


def resolve_target(target_id: str) -> tuple[str, list[str]]:
    if target_id in STEPS:
        return STEPS[target_id]["title"], [target_id]
    if target_id in GROUPS:
        group = GROUPS[target_id]
        return group["title"], list(group["steps"])
    raise KeyError(target_id)


def execute_step(run_id: str, step_id: str) -> int:
    step = STEPS[step_id]
    command = step["command"]
    append_output(run_id, f"\n$ {shlex.join(command)}\n")

    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    for line in process.stdout:
        append_output(run_id, line)

    exit_code = process.wait()
    append_output(run_id, f"\n[exit_code={exit_code}] {step['title']}\n")
    return exit_code


def run_target(run_id: str, step_ids: list[str]) -> None:
    exit_code = 0
    for index, step_id in enumerate(step_ids, start=1):
        with lock:
            runs[run_id]["active_step"] = step_id
            runs[run_id]["active_index"] = index

        step = STEPS[step_id]
        append_output(
            run_id,
            f"\n=== [{index}/{len(step_ids)}] {step['layer']} / {step['title']} ===\n",
        )
        exit_code = execute_step(run_id, step_id)
        if exit_code != 0:
            append_output(run_id, "\nPipeline stopped because this step failed.\n")
            mark_run(run_id, "failed", exit_code)
            return

    append_output(run_id, "\nAll selected steps completed successfully.\n")
    mark_run(run_id, "success", exit_code)


def load_bi_metrics() -> tuple[dict[str, Any], HTTPStatus]:
    command = cmd("./scripts/query-bi-metrics.sh")
    try:
        process = subprocess.run(
            command,
            cwd=ROOT,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=BI_QUERY_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return (
            {
                "error": "BI metrics query timed out",
                "output": exc.stdout or "",
            },
            HTTPStatus.GATEWAY_TIMEOUT,
        )

    marker_line = ""
    for line in process.stdout.splitlines():
        if line.startswith(BI_JSON_PREFIX):
            marker_line = line

    if process.returncode != 0:
        return (
            {
                "error": "BI metrics query failed",
                "exit_code": process.returncode,
                "output": process.stdout[-MAX_OUTPUT_CHARS:],
            },
            HTTPStatus.SERVICE_UNAVAILABLE,
        )

    if not marker_line:
        return (
            {
                "error": "BI metrics payload was not found",
                "output": process.stdout[-MAX_OUTPUT_CHARS:],
            },
            HTTPStatus.SERVICE_UNAVAILABLE,
        )

    payload = json.loads(marker_line.removeprefix(BI_JSON_PREFIX))
    payload["loaded_at"] = time.time()
    return payload, HTTPStatus.OK


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "DE5LiteDashboard/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        print(f"{self.address_string()} - {format % args}")

    def send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self) -> None:
        body = DASHBOARD_HTML.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in {"/", "/index.html"}:
            self.send_html()
            return

        if path == "/api/steps":
            with lock:
                active_run = current_run_id
            self.send_json(
                {
                    "groups": [public_group(group_id) for group_id in GROUPS],
                    "steps": [public_step(step_id) for step_id in STEPS],
                    "active_run_id": active_run,
                }
            )
            return

        if path == "/api/bi":
            with lock:
                active_run = current_run_id
                active = runs.get(active_run, {}) if active_run else {}
            if active.get("status") == "running":
                self.send_json(
                    {
                        "error": "pipeline run is active",
                        "active_run_id": active_run,
                    },
                    HTTPStatus.CONFLICT,
                )
                return

            payload, status = load_bi_metrics()
            self.send_json(payload, status)
            return

        if path.startswith("/api/runs/"):
            run_id = unquote(path.rsplit("/", 1)[-1])
            with lock:
                run = runs.get(run_id)
                payload = dict(run) if run else None
            if payload is None:
                self.send_json({"error": "run not found"}, HTTPStatus.NOT_FOUND)
                return
            self.send_json(payload)
            return

        self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        global current_run_id
        path = urlparse(self.path).path
        if not path.startswith("/api/run/"):
            self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            return

        target_id = unquote(path.rsplit("/", 1)[-1])
        try:
            target_title, step_ids = resolve_target(target_id)
        except KeyError:
            self.send_json({"error": "unknown target"}, HTTPStatus.NOT_FOUND)
            return

        with lock:
            if current_run_id is not None:
                active = runs.get(current_run_id, {})
                if active.get("status") == "running":
                    self.send_json(
                        {
                            "error": "another run is active",
                            "active_run_id": current_run_id,
                        },
                        HTTPStatus.CONFLICT,
                    )
                    return

            run_id = uuid.uuid4().hex
            now = time.time()
            runs[run_id] = {
                "id": run_id,
                "target_id": target_id,
                "target_title": target_title,
                "step_ids": step_ids,
                "status": "running",
                "active_step": None,
                "active_index": 0,
                "total_steps": len(step_ids),
                "output": "",
                "truncated": False,
                "exit_code": None,
                "created_at": now,
                "updated_at": now,
                "finished_at": None,
            }
            current_run_id = run_id

        thread = threading.Thread(target=run_target, args=(run_id, step_ids), daemon=True)
        thread.start()
        self.send_json({"run_id": run_id, "status": "running"})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the DE5 Lite Cycle dashboard.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8765")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not DASHBOARD_HTML.exists():
        raise FileNotFoundError(DASHBOARD_HTML)

    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"DE5 Lite Cycle dashboard: http://{args.host}:{args.port}")
    print("Only whitelisted local scripts can be executed from this dashboard.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
