#!/usr/bin/env python3
"""Seed a small OpenMetadata Data Quality demo for the DE5 final session.

This does not execute the real Spark/Great Expectations checks. It publishes
representative OpenMetadata Test Cases and Test Case Results so the class can
see how DQ appears next to table metadata and lineage.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


HOST = os.getenv("OPENMETADATA_HOST", "http://openmetadata_server:8585/api").rstrip("/")
ADMIN_EMAIL = os.getenv("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
ADMIN_PASSWORD = os.getenv("OPENMETADATA_ADMIN_PASSWORD", "admin")
INCLUDE_FAILURE = os.getenv("DQ_DEMO_INCLUDE_FAILURE", "false").lower() in {
    "1",
    "true",
    "yes",
}

CATEGORY_DAILY_FQN = (
    "de5_lakehouse_demo.de5_lite_pipeline.analytics.commerce_category_daily"
)
DAILY_FQN = "de5_lakehouse_demo.de5_lite_pipeline.analytics.commerce_event_type_daily"
BRONZE_FQN = "de5_lakehouse_demo.de5_lite_pipeline.bronze.commerce_events_bronze"


def api(path: str) -> str:
    if HOST.endswith("/api"):
        return f"{HOST}{path}"
    return f"{HOST}/api{path}"


def request_json(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(api(path), data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed {exc.code}: {body}") from exc
    return json.loads(body) if body else {}


def is_not_found(exc: Exception) -> bool:
    return " failed 404:" in str(exc)


def get_token() -> str:
    password = base64.b64encode(ADMIN_PASSWORD.encode("utf-8")).decode("ascii")
    response = request_json(
        "/v1/users/login",
        method="POST",
        payload={"email": ADMIN_EMAIL, "password": password},
    )
    token = response.get("accessToken")
    if not token:
        raise RuntimeError("OpenMetadata login did not return accessToken")
    return token


def safe_get(path: str, token: str) -> dict[str, Any] | None:
    try:
        return request_json(path, token=token)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except RuntimeError as exc:
        if is_not_found(exc):
            return None
        raise


def get_test_definition(name: str, token: str) -> dict[str, Any]:
    definition = safe_get(f"/v1/dataQuality/testDefinitions/name/{name}", token)
    if not definition:
        raise RuntimeError(f"Test definition not found: {name}")
    return definition


def table_exists(fqn: str, token: str) -> None:
    table = safe_get(f"/v1/tables/name/{urllib.parse.quote(fqn, safe='')}", token)
    if not table:
        raise RuntimeError(
            f"Table not found in OpenMetadata: {fqn}. "
            "Run ./seed-openmetadata-demo.sh first."
        )


def create_logical_suite(name: str, token: str) -> dict[str, Any]:
    payload = {
        "name": name,
        "displayName": "DE5 category daily serving quality",
        "description": (
            "Demo logical test suite for the DE5 class. It groups checks that "
            "protect the category daily BI serving table."
        ),
    }
    return request_json(
        "/v1/dataQuality/testSuites",
        method="PUT",
        payload=payload,
        token=token,
    )


def create_test_case(
    *,
    name: str,
    table_fqn: str,
    column: str | None,
    test_definition: dict[str, Any],
    parameter_values: list[dict[str, str]],
    token: str,
) -> dict[str, Any]:
    if column:
        entity_link = f"<#E::table::{table_fqn}::columns::{column}>"
    else:
        entity_link = f"<#E::table::{table_fqn}>"
    payload = {
        "entityLink": entity_link,
        "name": name,
        "description": f"DE5 demo quality test: {name}",
        "testDefinition": test_definition["fullyQualifiedName"],
        "parameterValues": parameter_values,
    }
    return request_json(
        "/v1/dataQuality/testCases",
        method="PUT",
        payload=payload,
        token=token,
    )


def add_cases_to_logical_suite(
    test_suite: dict[str, Any],
    test_cases: list[dict[str, Any]],
    token: str,
) -> dict[str, Any]:
    payload = {
        "testSuiteId": test_suite["id"],
        "testCaseIds": [test_case["id"] for test_case in test_cases],
    }
    return request_json(
        "/v1/dataQuality/testCases/logicalTestCases",
        method="PUT",
        payload=payload,
        token=token,
    )


def put_result(
    test_fqn: str,
    *,
    status: str,
    result: str,
    values: list[str],
    token: str,
) -> dict[str, Any]:
    payload = {
        "testCaseStatus": status,
        "result": result,
        "timestamp": int(time.time() * 1000),
        "testResultValue": [{"value": value} for value in values],
    }
    quoted = urllib.parse.quote(test_fqn, safe="")
    return request_json(
        f"/v1/dataQuality/testCases/testCaseResults/{quoted}",
        method="POST",
        payload=payload,
        token=token,
    )


def main() -> None:
    token = get_token()
    for table_fqn in [CATEGORY_DAILY_FQN, DAILY_FQN, BRONZE_FQN]:
        table_exists(table_fqn, token)

    suite = create_logical_suite(
        "de5_category_daily_serving_quality",
        token,
    )

    definitions = {
        name: get_test_definition(name, token)
        for name in [
            "tableRowCountToBeBetween",
            "columnValuesToBeNotNull",
            "columnValuesToBeBetween",
            "tableCustomSQLQuery",
        ]
    }

    test_cases = [
        create_test_case(
            name="category_daily_row_count_nonzero",
            table_fqn=CATEGORY_DAILY_FQN,
            column=None,
            test_definition=definitions["tableRowCountToBeBetween"],
            parameter_values=[{"name": "minValue", "value": "1"}],
            token=token,
        ),
        create_test_case(
            name="category_code_not_null",
            table_fqn=CATEGORY_DAILY_FQN,
            column="category_code",
            test_definition=definitions["columnValuesToBeNotNull"],
            parameter_values=[],
            token=token,
        ),
        create_test_case(
            name="purchase_count_non_negative",
            table_fqn=CATEGORY_DAILY_FQN,
            column="purchase_count",
            test_definition=definitions["columnValuesToBeBetween"],
            parameter_values=[{"name": "minValue", "value": "0"}],
            token=token,
        ),
        create_test_case(
            name="funnel_counts_are_monotonic",
            table_fqn=CATEGORY_DAILY_FQN,
            column=None,
            test_definition=definitions["tableCustomSQLQuery"],
            parameter_values=[
                {
                    "name": "sqlExpression",
                    "value": (
                        "SELECT event_date, category_code FROM {{ table_name }} "
                        "WHERE purchase_count > view_count OR cart_count > view_count"
                    ),
                },
                {"name": "strategy", "value": "ROWS"},
                {"name": "operator", "value": "=="},
                {"name": "threshold", "value": "0"},
            ],
            token=token,
        ),
    ]

    add_cases_to_logical_suite(suite, test_cases, token)

    for test_case in test_cases:
        name = test_case["fullyQualifiedName"]
        is_demo_failure = INCLUDE_FAILURE and name.endswith("funnel_counts_are_monotonic")
        put_result(
            name,
            status="Failed" if is_demo_failure else "Success",
            result=(
                "DE5 demo result: funnel rule failed intentionally for class demo."
                if is_demo_failure
                else "DE5 demo result: rule passed in the latest quality gate."
            ),
            values=["FAIL" if is_demo_failure else "PASS"],
            token=token,
        )
        print(
            "Seeded DQ test result: "
            f"{name} [{'FAIL' if is_demo_failure else 'PASS'}]"
        )

    print("\nOpenMetadata UI:")
    print("  http://localhost:8585")
    print("\nSearch:")
    print(f"  {CATEGORY_DAILY_FQN}")
    print("\nShow:")
    print("  Table -> Data Quality / Tests -> Lineage")
    if INCLUDE_FAILURE:
        print("\nReset to all PASS:")
        print("  ./seed-openmetadata-dq-demo.sh")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
