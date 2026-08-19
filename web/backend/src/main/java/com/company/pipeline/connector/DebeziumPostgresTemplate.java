package com.company.pipeline.connector;

import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;
import com.company.pipeline.pipeline.PipelineSnapshotMode;

/**
 * docs/kafka-webservice-design.md §11.2. pgoutput는 Postgres 10+ 내장 플러그인이라
 * wal2json 같은 별도 확장 설치가 필요 없다. slot.name/publication.name은 Postgres에서
 * 독점 리소스라 커넥터마다 고유해야 한다 - 커넥터명에서 파생시킨다.
 *
 * 사전 조건(DB 쪽에서 미리 돼 있어야 함, 이 렌더러가 대신 해주지 않음):
 *   - wal_level=logical (인스턴스 재시작 필요)
 *   - 소스 계정에 REPLICATION 권한
 */
@Component
public class DebeziumPostgresTemplate {

    public RenderedConnectorConfig render(SourceConnectorRequest request) {
        String connectorName = ConnectorNaming.sourceConnectorName(
                request.pipelineId(), "postgres", request.schema(), request.table());
        String tableIncludeList = request.schema() + "." + request.table();
        String slotAndPublicationName = ConnectorNaming.postgresSlotAndPublicationName(connectorName);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("tasks.max", "1");
        config.put("database.hostname", request.hostname());
        config.put("database.port", String.valueOf(request.port()));
        config.put("database.user", request.username());
        config.put("database.password", request.password());
        config.put("database.dbname", request.databaseName());
        config.put("topic.prefix", request.topicPrefix());
        config.put("plugin.name", "pgoutput");
        config.put("slot.name", slotAndPublicationName);
        config.put("publication.name", slotAndPublicationName);
        config.put("publication.autocreate.mode", "filtered");
        config.put("schema.include.list", request.schema());
        config.put("table.include.list", tableIncludeList);
        config.put("snapshot.mode", PipelineSnapshotMode.from(request.snapshotMode()).connectorValue());
        if (request.excludedColumns() != null && !request.excludedColumns().isBlank()) {
            config.put("column.exclude.list", java.util.Arrays.stream(request.excludedColumns().split(","))
                    .map(String::trim).filter(value -> !value.isEmpty())
                    .map(column -> tableIncludeList + "." + column)
                    .collect(java.util.stream.Collectors.joining(",")));
        }

        return new RenderedConnectorConfig(connectorName, "SOURCE",
                "io.debezium.connector.postgresql.PostgresConnector", config);
    }
}
