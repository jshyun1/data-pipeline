package com.company.pipeline.connector;

import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Debezium MySQL(binlog) 소스 커넥터 설정 렌더러.
 *
 * <p>사전 조건(DB 쪽에서 미리 돼 있어야 함, 이 렌더러가 대신 해주지 않음):
 *   - binlog_format=ROW
 *   - binlog_row_image=FULL
 *   - server_id 고유값 설정
 *   - 소스 계정에 SELECT, REPLICATION SLAVE/REPLICA, REPLICATION CLIENT 권한
 */
@Component
public class DebeziumMysqlTemplate {

    private static final String BOOTSTRAP_SERVERS = "kafka:9092";

    public RenderedConnectorConfig render(SourceConnectorRequest request) {
        String connectorName = ConnectorNaming.sourceConnectorName(
                request.pipelineId(), "mysql", request.schema(), request.table());
        String tableIncludeList = request.schema() + "." + request.table();

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        config.put("tasks.max", "1");
        config.put("database.hostname", request.hostname());
        config.put("database.port", String.valueOf(request.port()));
        config.put("database.user", request.username());
        config.put("database.password", request.password());
        config.put("database.server.id", String.valueOf(serverId(request.pipelineId())));
        config.put("topic.prefix", request.topicPrefix());
        config.put("database.include.list", request.schema());
        config.put("table.include.list", tableIncludeList);
        config.put("schema.history.internal.kafka.bootstrap.servers", BOOTSTRAP_SERVERS);
        config.put("schema.history.internal.kafka.topic", "schema-changes." + connectorName);
        config.put("include.schema.changes", "true");
        config.put("tombstones.on.delete", "false");
        config.put("decimal.handling.mode", "double");
        config.put("event.processing.failure.handling.mode", "warn");
        config.put("snapshot.mode", snapshotMode(request.snapshotMode()));

        return new RenderedConnectorConfig(connectorName, "SOURCE",
                "io.debezium.connector.mysql.MySqlConnector", config);
    }

    private int serverId(Long pipelineId) {
        return 54_000 + (int) Math.floorMod(pipelineId == null ? 0L : pipelineId, 100_000L);
    }

    private String snapshotMode(String snapshotMode) {
        return "NO_DATA".equalsIgnoreCase(snapshotMode) ? "no_data" : "initial";
    }
}
