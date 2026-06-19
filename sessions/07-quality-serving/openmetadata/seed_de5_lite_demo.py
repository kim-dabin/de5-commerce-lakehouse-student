#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.api.services.createDashboardService import (
    CreateDashboardServiceRequest,
)
from metadata.generated.schema.api.services.createMessagingService import (
    CreateMessagingServiceRequest,
)
from metadata.generated.schema.api.services.createPipelineService import (
    CreatePipelineServiceRequest,
)
from metadata.generated.schema.entity.data.dashboard import Dashboard, DashboardType
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.entity.data.pipeline import Pipeline, Task
from metadata.generated.schema.entity.data.topic import CleanupPolicy, Topic
from metadata.generated.schema.entity.services.connections.dashboard.customDashboardConnection import (
    CustomDashboardConnection,
    CustomDashboardType,
)
from metadata.generated.schema.entity.services.connections.database.common.basicAuth import (
    BasicAuth,
)
from metadata.generated.schema.entity.services.connections.database.mysqlConnection import (
    MysqlConnection,
)
from metadata.generated.schema.entity.services.connections.messaging.kafkaConnection import (
    KafkaConnection,
)
from metadata.generated.schema.entity.services.connections.pipeline.flinkConnection import (
    FlinkConnection,
)
from metadata.generated.schema.entity.services.connections.pipeline.sparkConnection import (
    SparkConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseServiceType,
)
from metadata.generated.schema.entity.services.dashboardService import (
    DashboardConnection,
    DashboardServiceType,
)
from metadata.generated.schema.entity.services.messagingService import (
    MessagingConnection,
    MessagingServiceType,
)
from metadata.generated.schema.entity.services.pipelineService import (
    PipelineConnection,
    PipelineServiceType,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.entityLineage import (
    ColumnLineage,
    EntitiesEdge,
    LineageDetails,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.schema import (
    DataTypeTopic,
    FieldModel,
    SchemaType,
    Topic as TopicSchema,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

try:
    from metadata.generated.schema.entity.services.connections.pipeline.airflowConnection import (
        AirflowConnection,
    )
    from metadata.generated.schema.entity.services.connections.pipeline.backendConnection import (
        BackendConnection,
    )
except Exception:  # pragma: no cover - import surface can move across OM versions.
    AirflowConnection = None
    BackendConnection = None


OPENMETADATA_HOST = os.getenv("OPENMETADATA_HOST", "http://openmetadata-server:8585/api")
ADMIN_EMAIL = os.getenv("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
ADMIN_PASSWORD = os.getenv("OPENMETADATA_ADMIN_PASSWORD", "admin")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("OPENMETADATA_KAFKA_BOOTSTRAP", "host.docker.internal:9092")
FLINK_HOST_PORT = os.getenv("OPENMETADATA_FLINK_HOST_PORT", "http://host.docker.internal:8081")
RESET_AIRFLOW_PIPELINE = os.getenv("OPENMETADATA_RESET_AIRFLOW_PIPELINE", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
SEED_LINEAGE = os.getenv("OPENMETADATA_SEED_LINEAGE", "true").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
SEED_AIRFLOW_PIPELINE = os.getenv("OPENMETADATA_SEED_AIRFLOW_PIPELINE", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}


def api_url(path: str) -> str:
    host = OPENMETADATA_HOST.rstrip("/")
    if host.endswith("/api"):
        return f"{host}{path}"
    return f"{host}/api{path}"


def find_token(payload: Any) -> str | None:
    if isinstance(payload, str) and payload.count(".") == 2:
        return payload
    if isinstance(payload, dict):
        for key in (
            "accessToken",
            "access_token",
            "idToken",
            "id_token",
            "jwtToken",
            "jwt_token",
            "token",
        ):
            value = payload.get(key)
            if isinstance(value, str) and value.count(".") == 2:
                return value
        for value in payload.values():
            token = find_token(value)
            if token:
                return token
    if isinstance(payload, list):
        for value in payload:
            token = find_token(value)
            if token:
                return token
    return None


def post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw) if raw else {}


def try_login_for_token() -> str | None:
    encoded_password = base64.b64encode(ADMIN_PASSWORD.encode("utf-8")).decode("ascii")
    login_payloads = [
        {"email": ADMIN_EMAIL, "password": encoded_password},
        {"username": ADMIN_EMAIL, "password": encoded_password},
    ]
    login_paths = ["/v1/users/login", "/v1/users/login/password"]

    for path in login_paths:
        for payload in login_payloads:
            try:
                response = post_json(api_url(path), payload)
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
                continue
            token = find_token(response)
            if token:
                return token
    return None


def get_token() -> str:
    env_token = os.getenv("OPENMETADATA_JWT_TOKEN") or os.getenv("OM_JWT_TOKEN")
    if env_token:
        return env_token

    login_token = try_login_for_token()
    if login_token:
        return login_token

    raise RuntimeError(
        "No OpenMetadata JWT token available. "
        "Log in to OpenMetadata, generate an access token from your profile, then run: "
        "OPENMETADATA_JWT_TOKEN='<token>' ./scripts/seed-openmetadata-demo.sh"
    )


def metadata_client() -> OpenMetadata:
    server_config = OpenMetadataConnection(
        hostPort=OPENMETADATA_HOST,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=get_token()),
    )
    client = OpenMetadata(server_config)
    client.health_check()
    return client


def col(name: str, data_type: DataType, description: str | None = None) -> Column:
    return Column(name=name, dataType=data_type, description=description)


def topic_field(
    name: str,
    data_type: DataTypeTopic,
    description: str | None = None,
) -> FieldModel:
    return FieldModel(name=name, dataType=data_type, description=description)


def create_or_update(client: OpenMetadata, request: Any) -> Any:
    return client.create_or_update(data=request)


def fqn_text(entity_or_fqn: Any) -> str:
    value = getattr(entity_or_fqn, "fullyQualifiedName", entity_or_fqn)
    return getattr(value, "root", str(value))


def entity_type(entity: Any) -> str:
    type_name = entity.__class__.__name__
    entity_types = {
        "Dashboard": "dashboard",
        "Table": "table",
        "Topic": "topic",
        "Pipeline": "pipeline",
    }
    if type_name not in entity_types:
        raise ValueError(f"Unsupported lineage entity type: {type_name}")
    return entity_types[type_name]


def create_pipeline(client: OpenMetadata) -> Any | None:
    if AirflowConnection is None or BackendConnection is None:
        print("Pipeline connection classes were not found; skipping pipeline entity.")
        return None

    pipeline_service = create_or_update(
        client,
        CreatePipelineServiceRequest(
            name="de5_airflow",
            displayName="DE5 Airflow",
            description="Local Airflow service orchestrating the DE5 lite lakehouse pipeline.",
            serviceType=PipelineServiceType.Airflow,
            connection=PipelineConnection(
                config=AirflowConnection(
                    hostPort="http://localhost:8080",
                    connection=BackendConnection(),
                )
            ),
        ),
    )

    return create_or_update(
        client,
        CreatePipelineRequest(
            name="de5_lite_lakehouse_pipeline",
            displayName="DE5 Lite Lakehouse Pipeline",
            description=(
                "Teaching DAG that runs Kafka -> StarRocks realtime OLAP and "
                "Kafka -> Flink/Paimon -> Spark/Iceberg batch lakehouse paths."
            ),
            service=pipeline_service.fullyQualifiedName,
        ),
    )


def create_processing_pipelines(client: OpenMetadata) -> tuple[Any, Any]:
    flink_service = create_or_update(
        client,
        CreatePipelineServiceRequest(
            name="de5_flink",
            displayName="DE5 Flink",
            description="Local Flink SQL engine that consumes Kafka and writes Paimon Bronze.",
            serviceType=PipelineServiceType.Flink,
            connection=PipelineConnection(
                config=FlinkConnection(hostPort=FLINK_HOST_PORT),
            ),
        ),
    )
    spark_service = create_or_update(
        client,
        CreatePipelineServiceRequest(
            name="de5_spark",
            displayName="DE5 Spark",
            description="Local Spark batch engine that transforms Paimon Bronze into Iceberg analytics tables.",
            serviceType=PipelineServiceType.Spark,
            connection=PipelineConnection(
                config=SparkConnection(),
            ),
        ),
    )

    flink_pipeline = create_or_update(
        client,
        CreatePipelineRequest(
            name="commerce_events_bronze_ingestion",
            displayName="Flink SQL: Kafka to Paimon Bronze",
            description="Flink SQL job that reads commerce-events from Kafka and writes commerce_events_bronze to Paimon.",
            service=flink_service.fullyQualifiedName,
            sourceUrl=FLINK_HOST_PORT,
            tasks=[
                Task(
                    name="insert_commerce_events_bronze",
                    displayName="Insert Commerce Events Bronze",
                    taskType="Flink SQL",
                    taskSQL=(
                        "INSERT INTO paimon_lake.bronze.commerce_events_bronze "
                        "SELECT JSON_VALUE(raw_json, '$.id'), ... FROM commerce_events_kafka_raw"
                    ),
                )
            ],
        ),
    )
    spark_pipeline = create_or_update(
        client,
        CreatePipelineRequest(
            name="commerce_events_iceberg_transform",
            displayName="Spark Batch: Paimon Bronze to Iceberg Analytics",
            description="Spark batch job that reads Paimon Bronze and writes clean and aggregate Iceberg tables.",
            service=spark_service.fullyQualifiedName,
            tasks=[
                Task(
                    name="transform_commerce_events_iceberg",
                    displayName="Transform Commerce Events to Iceberg",
                    taskType="Spark",
                    taskSQL=(
                        "CREATE TABLE iceberg_lake.analytics.commerce_events_clean AS "
                        "SELECT ... FROM commerce_events_bronze"
                    ),
                )
            ],
        ),
    )

    return flink_pipeline, spark_pipeline


def create_streamlit_dashboard(client: OpenMetadata) -> Any:
    dashboard_service = create_or_update(
        client,
        CreateDashboardServiceRequest(
            name="de5_streamlit",
            displayName="DE5 Streamlit BI",
            description=(
                "Local Streamlit dashboard used in class to compare Realtime OLAP "
                "metrics from StarRocks with Batch Lakehouse BI metrics from Iceberg."
            ),
            serviceType=DashboardServiceType.CustomDashboard,
            connection=DashboardConnection(
                config=CustomDashboardConnection(type=CustomDashboardType.CustomDashboard),
            ),
        ),
    )

    return create_or_update(
        client,
        CreateDashboardRequest(
            name="commerce_lakehouse_bi",
            displayName="Streamlit BI: Commerce Lakehouse Dashboard",
            description=(
                "Classroom BI dashboard. The Realtime Ops tab reads StarRocks "
                "serving views, while the Daily Business tab reads Iceberg analytics "
                "tables. It is the final student-visible output of the lite pipeline."
            ),
            dashboardType=DashboardType.Dashboard,
            sourceUrl="http://127.0.0.1:8501",
            service=dashboard_service.fullyQualifiedName,
        ),
    )


def delete_airflow_pipeline_if_exists(client: OpenMetadata) -> None:
    pipeline = client.get_by_name(
        entity=Pipeline,
        fqn="de5_airflow.de5_lite_lakehouse_pipeline",
    )
    if pipeline is None:
        return
    client.delete(entity=Pipeline, entity_id=pipeline.id, recursive=True, hard_delete=True)
    print("Deleted existing Airflow pipeline entity for automatic lineage refresh.")


def delete_legacy_kafka_topic_proxy_if_exists(client: OpenMetadata) -> None:
    legacy_table = client.get_by_name(
        entity=Table,
        fqn="de5_lakehouse_demo.de5_lite_pipeline.kafka.commerce_events_topic",
    )
    if legacy_table is None:
        return
    client.delete(entity=Table, entity_id=legacy_table.id, recursive=True, hard_delete=True)
    print("Deleted legacy table-like Kafka topic proxy.")


def delete_entity_by_name_if_exists(client: OpenMetadata, entity: Any, fqn: str) -> None:
    existing = client.get_by_name(entity=entity, fqn=fqn)
    if existing is None:
        return
    client.delete(entity=entity, entity_id=existing.id, recursive=True, hard_delete=True)
    print(f"Deleted legacy entity: {fqn}")


def delete_legacy_github_demo_assets_if_exists(client: OpenMetadata) -> None:
    for entity, fqn in [
        (Pipeline, "de5_flink.github_events_bronze_ingestion"),
        (Pipeline, "de5_spark.github_events_iceberg_transform"),
        (Dashboard, "de5_streamlit.commerce_lakehouse_bi"),
        (Table, "de5_lakehouse_demo.de5_lite_pipeline.bronze.github_events_bronze"),
        (Table, "de5_lakehouse_demo.de5_lite_pipeline.analytics.github_events_clean"),
        (Table, "de5_lakehouse_demo.de5_lite_pipeline.analytics.github_event_type_daily"),
        (Topic, "de5_kafka.github-events"),
    ]:
        delete_entity_by_name_if_exists(client, entity, fqn)


def lineage_has_edge(client: OpenMetadata, source: Any, target: Any) -> bool:
    lineage = client.get_lineage_by_id(
        entity=source.__class__,
        entity_id=source.id,
        up_depth=0,
        down_depth=1,
    )
    if not lineage:
        return False
    source_id = str(getattr(source.id, "root", source.id))
    target_id = str(getattr(target.id, "root", target.id))
    return any(
        edge.get("fromEntity") == source_id and edge.get("toEntity") == target_id
        for edge in lineage.get("downstreamEdges") or []
    )


def lineage_details(
    sql: str,
    pipeline_entity: Any | None,
    columns_lineage: list[ColumnLineage] | None = None,
) -> LineageDetails:
    kwargs: dict[str, Any] = {"sqlQuery": sql}
    if columns_lineage:
        kwargs["columnsLineage"] = columns_lineage
    if pipeline_entity is not None:
        kwargs["pipeline"] = EntityReference(id=pipeline_entity.id, type="pipeline")
    return LineageDetails(**kwargs)


def add_lineage(
    client: OpenMetadata,
    source: Any,
    target: Any,
    description: str,
    details: LineageDetails,
) -> None:
    request = AddLineageRequest(
        edge=EntitiesEdge(
            description=description,
            fromEntity=EntityReference(id=source.id, type=entity_type(source)),
            toEntity=EntityReference(id=target.id, type=entity_type(target)),
            lineageDetails=details,
        )
    )
    client.add_lineage(data=request)


def delete_lineage_edge_if_exists(client: OpenMetadata, source: Any, target: Any) -> None:
    if not lineage_has_edge(client, source, target):
        return
    edge = EntitiesEdge(
        fromEntity=EntityReference(id=source.id, type=entity_type(source)),
        toEntity=EntityReference(id=target.id, type=entity_type(target)),
    )
    try:
        client.delete_lineage_edge(edge=edge)
    except Exception:
        return


def delete_demo_lineage_edges(client: OpenMetadata, entities: list[Any]) -> None:
    # The Airflow lineage provider groups DAG-wide xlets by key. During local
    # iteration we may create noisy duplicate/cross-product edges, so automatic
    # lineage mode starts from a clean demo graph.
    entity_by_id = {str(getattr(entity.id, "root", entity.id)): entity for entity in entities}
    seen_edges: set[tuple[str, str]] = set()

    for entity in entities:
        lineage = client.get_lineage_by_id(
            entity=entity.__class__,
            entity_id=entity.id,
            up_depth=3,
            down_depth=3,
        )
        if not lineage:
            continue

        for edge in (lineage.get("upstreamEdges") or []) + (lineage.get("downstreamEdges") or []):
            source_id = edge.get("fromEntity")
            target_id = edge.get("toEntity")
            if source_id not in entity_by_id or target_id not in entity_by_id:
                continue
            edge_key = (source_id, target_id)
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)
            delete_lineage_edge_if_exists(
                client,
                entity_by_id[source_id],
                entity_by_id[target_id],
            )


def seed_processing_lineage(
    client: OpenMetadata,
    commerce_events_topic: Any,
    bronze: Any,
    clean: Any,
    daily: Any,
    category_daily: Any,
    flink_pipeline: Any,
    spark_pipeline: Any,
) -> None:
    pairs = [
        (commerce_events_topic, flink_pipeline),
        (flink_pipeline, bronze),
        (bronze, spark_pipeline),
        (spark_pipeline, clean),
        (clean, daily),
        (clean, category_daily),
    ]
    for source, target in pairs:
        delete_lineage_edge_if_exists(client, source, target)

    add_lineage(
        client,
        commerce_events_topic,
        flink_pipeline,
        "Flink SQL consumes Kafka messages from commerce-events.",
        lineage_details(
            sql="CREATE TEMPORARY TABLE commerce_events_kafka_raw (...) WITH ('connector' = 'kafka')",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        flink_pipeline,
        bronze,
        "Flink SQL writes parsed events into the Paimon Bronze table.",
        lineage_details(
            sql="INSERT INTO paimon_lake.bronze.commerce_events_bronze SELECT ...",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        bronze,
        spark_pipeline,
        "Spark reads Paimon Bronze as the batch transform input.",
        lineage_details(
            sql="spark.read.table('paimon_lake.bronze.commerce_events_bronze')",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        spark_pipeline,
        clean,
        "Spark writes the clean Iceberg events table.",
        lineage_details(
            sql="CREATE TABLE iceberg_lake.analytics.commerce_events_clean AS SELECT ...",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        clean,
        daily,
        "Spark aggregates clean Iceberg events by event date and type.",
        lineage_details(
            sql="CREATE TABLE iceberg_lake.analytics.commerce_event_type_daily AS SELECT ... GROUP BY ...",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        clean,
        category_daily,
        "Spark aggregates clean Iceberg events by event date and category.",
        lineage_details(
            sql="CREATE TABLE iceberg_lake.analytics.commerce_category_daily AS SELECT ... GROUP BY ...",
            pipeline_entity=None,
        ),
    )


def seed_realtime_lineage(
    client: OpenMetadata,
    commerce_events_topic: Any,
    realtime_events: Any,
    realtime_event_type: Any,
    realtime_category: Any,
    realtime_minute: Any,
    pipeline: Any | None,
) -> None:
    pairs = [
        (commerce_events_topic, realtime_events),
        (realtime_events, realtime_event_type),
        (realtime_events, realtime_category),
        (realtime_events, realtime_minute),
    ]
    for source, target in pairs:
        delete_lineage_edge_if_exists(client, source, target)

    add_lineage(
        client,
        commerce_events_topic,
        realtime_events,
        "StarRocks Stream Load consumes replayed Kafka commerce events for realtime OLAP serving.",
        lineage_details(
            sql=(
                "STREAM LOAD de5_realtime_olap.commerce_events_rt "
                "FROM Kafka topic commerce-events JSONEachRow"
            ),
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        realtime_events,
        realtime_event_type,
        "StarRocks view aggregates the realtime event table by event type.",
        lineage_details(
            sql=(
                "CREATE VIEW commerce_event_type_realtime AS "
                "SELECT event_type, COUNT(*), COUNT(DISTINCT user_id), SUM(...) "
                "FROM commerce_events_rt_typed GROUP BY event_type"
            ),
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        realtime_events,
        realtime_category,
        "StarRocks view aggregates the realtime event table by category.",
        lineage_details(
            sql=(
                "CREATE VIEW commerce_category_realtime AS "
                "SELECT category_code, COUNT(*), SUM(IF(event_type = 'purchase', price, 0)) "
                "FROM commerce_events_rt_typed GROUP BY category_code"
            ),
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        realtime_events,
        realtime_minute,
        "StarRocks view aggregates the realtime event table by minute and event type.",
        lineage_details(
            sql=(
                "CREATE VIEW commerce_minute_event_type_realtime AS "
                "SELECT event_minute, event_type, COUNT(*) "
                "FROM commerce_events_rt_typed GROUP BY event_minute, event_type"
            ),
            pipeline_entity=None,
        ),
    )


def seed_bi_lineage(
    client: OpenMetadata,
    realtime_event_type: Any,
    realtime_category: Any,
    realtime_minute: Any,
    daily: Any,
    category_daily: Any,
    dashboard: Any,
    pipeline: Any | None,
) -> None:
    pairs = [
        (realtime_event_type, dashboard),
        (realtime_category, dashboard),
        (realtime_minute, dashboard),
        (daily, dashboard),
        (category_daily, dashboard),
    ]
    for source, target in pairs:
        delete_lineage_edge_if_exists(client, source, target)

    add_lineage(
        client,
        realtime_event_type,
        dashboard,
        "Streamlit Realtime Ops tab visualizes event-type metrics served by StarRocks.",
        lineage_details(
            sql="SELECT * FROM de5_realtime_olap.commerce_event_type_realtime",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        realtime_category,
        dashboard,
        "Streamlit Realtime Ops tab visualizes category metrics served by StarRocks.",
        lineage_details(
            sql="SELECT * FROM de5_realtime_olap.commerce_category_realtime",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        realtime_minute,
        dashboard,
        "Streamlit Realtime Ops tab visualizes minute-level event metrics served by StarRocks.",
        lineage_details(
            sql="SELECT * FROM de5_realtime_olap.commerce_minute_event_type_realtime",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        daily,
        dashboard,
        "Streamlit Daily Business tab visualizes daily event-type metrics from Iceberg.",
        lineage_details(
            sql="SELECT * FROM iceberg_lake.analytics.commerce_event_type_daily",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        category_daily,
        dashboard,
        "Streamlit Daily Business tab visualizes daily category metrics from Iceberg.",
        lineage_details(
            sql="SELECT * FROM iceberg_lake.analytics.commerce_category_daily",
            pipeline_entity=None,
        ),
    )


def seed_airflow_orchestration_lineage(
    client: OpenMetadata,
    commerce_events_topic: Any,
    airflow_pipeline: Any,
    realtime_events: Any,
    flink_pipeline: Any,
    spark_pipeline: Any,
) -> None:
    pairs = [
        (commerce_events_topic, airflow_pipeline),
        (airflow_pipeline, realtime_events),
        (airflow_pipeline, flink_pipeline),
        (airflow_pipeline, spark_pipeline),
    ]
    for source, target in pairs:
        delete_lineage_edge_if_exists(client, source, target)

    add_lineage(
        client,
        commerce_events_topic,
        airflow_pipeline,
        "Airflow DAG starts from sample commerce events produced to Kafka.",
        lineage_details(
            sql="DAG task: reset_kafka_topic -> produce_sample_events",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        airflow_pipeline,
        realtime_events,
        "Airflow orchestrates the StarRocks realtime OLAP load task.",
        lineage_details(
            sql="DAG task: load_starrocks_realtime_olap_from_kafka",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        airflow_pipeline,
        flink_pipeline,
        "Airflow orchestrates the Flink SQL Bronze ingestion task.",
        lineage_details(
            sql="DAG task: run_flink_paimon_bronze",
            pipeline_entity=None,
        ),
    )
    add_lineage(
        client,
        airflow_pipeline,
        spark_pipeline,
        "Airflow orchestrates the Spark Iceberg transform task.",
        lineage_details(
            sql="DAG task: run_spark_iceberg_transform",
            pipeline_entity=None,
        ),
    )


def main() -> int:
    client = metadata_client()

    messaging_service = create_or_update(
        client,
        CreateMessagingServiceRequest(
            name="de5_kafka",
            displayName="DE5 Kafka",
            description=(
                "Local Kafka broker used by the DE5 lite pipeline. "
                "The actual demo topic is commerce-events."
            ),
            serviceType=MessagingServiceType.Kafka,
            connection=MessagingConnection(
                config=KafkaConnection(bootstrapServers=KAFKA_BOOTSTRAP_SERVERS)
            ),
        ),
    )

    commerce_events_topic = create_or_update(
        client,
        CreateTopicRequest(
            name="commerce-events",
            displayName="Kafka Topic: commerce-events",
            description=(
                "Actual Kafka topic used by the lite pipeline. "
                "The producer writes raw commerce event JSON messages here before Flink consumes them."
            ),
            service=messaging_service.fullyQualifiedName,
            partitions=3,
            replicationFactor=1,
            cleanupPolicies=[CleanupPolicy.delete],
            messageSchema=TopicSchema(
                schemaType=SchemaType.JSON,
                schemaFields=[
                    topic_field("id", DataTypeTopic.STRING, "Event id added for lakehouse processing."),
                    topic_field("event_time", DataTypeTopic.STRING, "Original event timestamp text."),
                    topic_field("event_type", DataTypeTopic.STRING, "Commerce behavior: view, cart, remove_from_cart, purchase."),
                    topic_field("product_id", DataTypeTopic.LONG, "Product id."),
                    topic_field("category_id", DataTypeTopic.LONG, "Product category id."),
                    topic_field("category_code", DataTypeTopic.STRING, "Product category path."),
                    topic_field("brand", DataTypeTopic.STRING, "Product brand."),
                    topic_field("price", DataTypeTopic.DOUBLE, "Product price."),
                    topic_field("user_id", DataTypeTopic.LONG, "Shopper id."),
                    topic_field("user_session", DataTypeTopic.STRING, "User session id."),
                ],
            ),
        ),
    )

    db_service = create_or_update(
        client,
        CreateDatabaseServiceRequest(
            name="de5_lakehouse_demo",
            displayName="DE5 Lakehouse Demo",
            description=(
                "Mentor-only OpenMetadata demo service for the DE5 lite pipeline. "
                "It models Kafka, Paimon Bronze, StarRocks Realtime OLAP, "
                "Iceberg Analytics, and Airflow lineage."
            ),
            serviceType=DatabaseServiceType.Mysql,
            connection=DatabaseConnection(
                config=MysqlConnection(
                    username="demo",
                    authType=BasicAuth(password="demo"),
                    hostPort="localhost:15432",
                )
            ),
        ),
    )

    database = create_or_update(
        client,
        CreateDatabaseRequest(
            name="de5_lite_pipeline",
            description="Logical database grouping the bootcamp lite pipeline assets.",
            service=db_service.fullyQualifiedName,
        ),
    )

    schemas = {}
    for schema_name, description in {
        "bronze": "Bronze storage layer backed by Paimon in the lite pipeline.",
        "realtime_olap": "Realtime OLAP serving layer backed by StarRocks in the lite pipeline.",
        "analytics": "Analytics layer backed by Iceberg tables in the lite pipeline.",
    }.items():
        schemas[schema_name] = create_or_update(
            client,
            CreateDatabaseSchemaRequest(
                name=schema_name,
                description=description,
                database=database.fullyQualifiedName,
            ),
        )

    delete_legacy_kafka_topic_proxy_if_exists(client)
    delete_legacy_github_demo_assets_if_exists(client)
    flink_pipeline, spark_pipeline = create_processing_pipelines(client)
    streamlit_dashboard = create_streamlit_dashboard(client)

    bronze = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_events_bronze",
            displayName="Paimon Bronze: commerce_events_bronze",
            description="Bronze table written by Flink SQL after parsing Kafka raw JSON.",
            databaseSchema=schemas["bronze"].fullyQualifiedName,
            columns=[
                col("event_id", DataType.STRING),
                col("event_type", DataType.STRING),
                col("event_time_text", DataType.STRING),
                col("product_id", DataType.BIGINT),
                col("category_id", DataType.BIGINT),
                col("category_code", DataType.STRING),
                col("brand", DataType.STRING),
                col("price", DataType.DECIMAL),
                col("user_id", DataType.BIGINT),
                col("user_session", DataType.STRING),
                col("raw_json", DataType.STRING),
                col("ingested_at", DataType.TIMESTAMP),
            ],
        ),
    )

    realtime_events = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_events_rt",
            displayName="StarRocks Realtime OLAP: commerce_events_rt",
            description="Realtime OLAP table loaded from Kafka events through StarRocks Stream Load.",
            databaseSchema=schemas["realtime_olap"].fullyQualifiedName,
            columns=[
                col("id", DataType.STRING),
                col("event_time", DataType.STRING),
                col("event_type", DataType.STRING),
                col("product_id", DataType.BIGINT),
                col("category_id", DataType.BIGINT),
                col("category_code", DataType.STRING),
                col("brand", DataType.STRING),
                col("price", DataType.DECIMAL),
                col("user_id", DataType.BIGINT),
                col("user_session", DataType.STRING),
                col("ingested_at", DataType.TIMESTAMP),
            ],
        ),
    )

    realtime_event_type = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_event_type_realtime",
            displayName="StarRocks Realtime OLAP: commerce_event_type_realtime",
            description="Realtime event-type aggregate view served from StarRocks.",
            databaseSchema=schemas["realtime_olap"].fullyQualifiedName,
            columns=[
                col("event_type", DataType.STRING),
                col("event_count", DataType.BIGINT),
                col("user_count", DataType.BIGINT),
                col("session_count", DataType.BIGINT),
                col("product_count", DataType.BIGINT),
                col("revenue", DataType.DECIMAL),
                col("first_event_at", DataType.TIMESTAMP),
                col("last_event_at", DataType.TIMESTAMP),
            ],
        ),
    )

    realtime_category = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_category_realtime",
            displayName="StarRocks Realtime OLAP: commerce_category_realtime",
            description="Realtime category aggregate view served from StarRocks.",
            databaseSchema=schemas["realtime_olap"].fullyQualifiedName,
            columns=[
                col("category_code", DataType.STRING),
                col("event_count", DataType.BIGINT),
                col("view_count", DataType.BIGINT),
                col("cart_count", DataType.BIGINT),
                col("purchase_count", DataType.BIGINT),
                col("user_count", DataType.BIGINT),
                col("session_count", DataType.BIGINT),
                col("revenue", DataType.DECIMAL),
                col("last_event_at", DataType.TIMESTAMP),
            ],
        ),
    )

    realtime_minute = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_minute_event_type_realtime",
            displayName="StarRocks Realtime OLAP: commerce_minute_event_type_realtime",
            description="Minute-level realtime event-type aggregate view served from StarRocks.",
            databaseSchema=schemas["realtime_olap"].fullyQualifiedName,
            columns=[
                col("event_minute", DataType.TIMESTAMP),
                col("event_type", DataType.STRING),
                col("event_count", DataType.BIGINT),
                col("user_count", DataType.BIGINT),
                col("revenue", DataType.DECIMAL),
            ],
        ),
    )

    clean = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_events_clean",
            displayName="Iceberg Analytics: commerce_events_clean",
            description="Clean Iceberg table created by Spark from Paimon Bronze.",
            databaseSchema=schemas["analytics"].fullyQualifiedName,
            columns=[
                col("event_id", DataType.STRING),
                col("event_type", DataType.STRING),
                col("event_time_ts", DataType.TIMESTAMP),
                col("event_date", DataType.DATE),
                col("product_id", DataType.BIGINT),
                col("category_id", DataType.BIGINT),
                col("category_code", DataType.STRING),
                col("brand", DataType.STRING),
                col("price", DataType.DECIMAL),
                col("user_id", DataType.BIGINT),
                col("user_session", DataType.STRING),
                col("raw_json", DataType.STRING),
                col("ingested_at", DataType.TIMESTAMP),
            ],
        ),
    )

    daily = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_event_type_daily",
            displayName="Iceberg Analytics: commerce_event_type_daily",
            description="Daily event-type aggregate Iceberg table created by Spark.",
            databaseSchema=schemas["analytics"].fullyQualifiedName,
            columns=[
                col("event_date", DataType.DATE),
                col("event_type", DataType.STRING),
                col("event_count", DataType.BIGINT),
                col("user_count", DataType.BIGINT),
                col("session_count", DataType.BIGINT),
                col("product_count", DataType.BIGINT),
                col("revenue", DataType.DECIMAL),
                col("first_event_at", DataType.TIMESTAMP),
                col("last_event_at", DataType.TIMESTAMP),
            ],
        ),
    )

    category_daily = create_or_update(
        client,
        CreateTableRequest(
            name="commerce_category_daily",
            displayName="Iceberg Analytics: commerce_category_daily",
            description="Daily category aggregate Iceberg table created by Spark.",
            databaseSchema=schemas["analytics"].fullyQualifiedName,
            columns=[
                col("event_date", DataType.DATE),
                col("category_code", DataType.STRING),
                col("event_count", DataType.BIGINT),
                col("view_count", DataType.BIGINT),
                col("cart_count", DataType.BIGINT),
                col("purchase_count", DataType.BIGINT),
                col("user_count", DataType.BIGINT),
                col("session_count", DataType.BIGINT),
                col("revenue", DataType.DECIMAL),
            ],
        ),
    )

    if RESET_AIRFLOW_PIPELINE:
        delete_airflow_pipeline_if_exists(client)
        delete_demo_lineage_edges(
            client,
            [
                commerce_events_topic,
                bronze,
                realtime_events,
                realtime_event_type,
                realtime_category,
                realtime_minute,
                clean,
                daily,
                category_daily,
                flink_pipeline,
                spark_pipeline,
                streamlit_dashboard,
            ],
        )

    pipeline = create_pipeline(client) if SEED_LINEAGE and SEED_AIRFLOW_PIPELINE else None
    seed_realtime_lineage(
        client,
        commerce_events_topic,
        realtime_events,
        realtime_event_type,
        realtime_category,
        realtime_minute,
        pipeline,
    )
    seed_processing_lineage(
        client,
        commerce_events_topic,
        bronze,
        clean,
        daily,
        category_daily,
        flink_pipeline,
        spark_pipeline,
    )
    seed_bi_lineage(
        client,
        realtime_event_type,
        realtime_category,
        realtime_minute,
        daily,
        category_daily,
        streamlit_dashboard,
        pipeline,
    )
    if pipeline is not None:
        seed_airflow_orchestration_lineage(
            client,
            commerce_events_topic,
            pipeline,
            realtime_events,
            flink_pipeline,
            spark_pipeline,
        )

    if not SEED_LINEAGE:
        print("Seeded OpenMetadata DE5 lite pipeline demo assets.")
        print("Lineage creation skipped because OPENMETADATA_SEED_LINEAGE=false.")
        print(f"Kafka messaging service: {fqn_text(messaging_service)}")
        print(f"Kafka topic: {fqn_text(commerce_events_topic)}")
        print(f"Flink pipeline: {fqn_text(flink_pipeline)}")
        print(f"Spark pipeline: {fqn_text(spark_pipeline)}")
        print(f"Service: {fqn_text(db_service)}")
        print(f"Bronze table: {fqn_text(bronze)}")
        print(f"Realtime OLAP table: {fqn_text(realtime_events)}")
        print(f"Realtime event-type view: {fqn_text(realtime_event_type)}")
        print(f"Realtime category view: {fqn_text(realtime_category)}")
        print(f"Realtime minute view: {fqn_text(realtime_minute)}")
        print(f"Clean table: {fqn_text(clean)}")
        print(f"Daily aggregate table: {fqn_text(daily)}")
        print(f"Category aggregate table: {fqn_text(category_daily)}")
        print(f"Streamlit dashboard: {fqn_text(streamlit_dashboard)}")
        print("Airflow pipeline entity: skipped for classroom lineage clarity.")
        return 0

    print("Seeded OpenMetadata DE5 lite pipeline demo.")
    print(f"Kafka messaging service: {fqn_text(messaging_service)}")
    print(f"Kafka topic: {fqn_text(commerce_events_topic)}")
    print(f"Flink pipeline: {fqn_text(flink_pipeline)}")
    print(f"Spark pipeline: {fqn_text(spark_pipeline)}")
    print(f"Service: {fqn_text(db_service)}")
    print(f"Bronze table: {fqn_text(bronze)}")
    print(f"Realtime OLAP table: {fqn_text(realtime_events)}")
    print(f"Realtime event-type view: {fqn_text(realtime_event_type)}")
    print(f"Realtime category view: {fqn_text(realtime_category)}")
    print(f"Realtime minute view: {fqn_text(realtime_minute)}")
    print(f"Clean table: {fqn_text(clean)}")
    print(f"Daily aggregate table: {fqn_text(daily)}")
    print(f"Category aggregate table: {fqn_text(category_daily)}")
    print(f"Streamlit dashboard: {fqn_text(streamlit_dashboard)}")
    if pipeline is not None:
        print(f"Pipeline: {fqn_text(pipeline)}")
    else:
        print("Airflow pipeline entity: skipped for classroom lineage clarity.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
