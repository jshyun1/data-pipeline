package com.company.pipeline.connector;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.LogSinkConnectorRequest;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SinkConnectorRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * docs/kafka-webservice-design.md §11.3, §11.4. Debezium JDBC Sink는 Postgres/Oracle
 * 드라이버가 둘 다 플러그인 디렉터리에 번들돼 있어(kafka-connect/Dockerfile 참고 - 두
 * 드라이버 모두 debezium-debezium-connector-jdbc 디렉터리에 명시적으로 추가해야 함,
 * Kafka Connect의 플러그인별 클래스로더 격리 때문에 다른 플러그인 디렉터리의 jar는
 * 안 보임) 타겟 DB 종류와 무관하게 이 템플릿 하나로 처리한다 - connection.url만 방언별로 다르다.
 */
@Component
public class JdbcSinkTemplate {

    public RenderedConnectorConfig render(SinkConnectorRequest request) {
        String connectorName = ConnectorNaming.sinkConnectorName(
                request.pipelineId(), request.targetDbType().name().toLowerCase(),
                request.targetSchema(), request.targetTable());
        String topics = ConnectorNaming.topicName(
                request.topicPrefix(), request.sourceDbType(), request.sourceSchema(), request.sourceTable());

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector");
        config.put("tasks.max", "1");
        config.put("topics", topics);
        config.put("connection.url", connectionUrl(request));
        config.put("connection.username", request.username());
        config.put("connection.password", request.password());
        config.put("insert.mode", "upsert");
        config.put("primary.key.mode", "record_key");
        config.put("schema.evolution", "basic");
        config.put("table.name.format", request.targetSchema() + "." + request.targetTable());
        config.put("delete.enabled", String.valueOf(request.deleteEnabled()));
        addDlqSettings(config, request.pipelineId());

        return new RenderedConnectorConfig(connectorName, "SINK",
                "io.debezium.connector.jdbc.JdbcSinkConnector", config);
    }

    /**
     * 로그파일 적재 파이프라인용. Filebeat가 소스 역할을 하므로(Kafka Connect 커넥터
     * 아님) 이 싱크만 등록한다. CDC 싱크와 다른 점: 로그는 PK가 없는 append-only라
     * primary.key.mode=none/insert.mode=insert, delete는 항상 false, topics는
     * log_pipeline_source.topic_name을 그대로 쓴다(스키마.테이블 조합 아님). Filebeat가
     * Kafka Connect 스키마 봉투를 직접 만들어 보내므로 key.converter만 커넥터 단위로
     * StringConverter로 오버라이드한다(키는 안 씀) - value.converter는 worker 기본값
     * (JsonConverter, schemas.enable=true) 그대로 - CDC 커넥터들과 공유하는 값이라 안 건드림.
     */
    public RenderedConnectorConfig renderForLogPipeline(LogSinkConnectorRequest request) {
        String connectorName = ConnectorNaming.sinkConnectorName(
                request.pipelineId(), request.targetDbType().name().toLowerCase(),
                request.targetSchema(), request.targetTable());

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector");
        config.put("tasks.max", "1");
        config.put("topics", request.topicName());
        config.put("connection.url", connectionUrl(request.targetDbType(), request.hostname(),
                request.port(), request.databaseName(), request.serviceName()));
        config.put("connection.username", request.username());
        config.put("connection.password", request.password());
        config.put("insert.mode", "insert");
        config.put("primary.key.mode", "none");
        config.put("schema.evolution", "basic");
        config.put("table.name.format", request.targetSchema() + "." + request.targetTable());
        config.put("delete.enabled", "false");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        addDlqSettings(config, request.pipelineId());

        return new RenderedConnectorConfig(connectorName, "SINK",
                "io.debezium.connector.jdbc.JdbcSinkConnector", config);
    }

    private void addDlqSettings(Map<String, Object> config, Long pipelineId) {
        config.put("errors.tolerance", "all");
        config.put("errors.deadletterqueue.topic.name", "dlq.pipeline-" + pipelineId);
        config.put("errors.deadletterqueue.topic.replication.factor", "1");
        config.put("errors.deadletterqueue.context.headers.enable", "true");
        config.put("errors.log.enable", "true");
        // 원문 데이터가 Connect 로그에 평문으로 남지 않게 한다.
        config.put("errors.log.include.messages", "false");
    }

    private String connectionUrl(SinkConnectorRequest request) {
        return connectionUrl(request.targetDbType(), request.hostname(), request.port(),
                request.databaseName(), request.serviceName());
    }

    private String connectionUrl(DbType targetDbType, String hostname, int port,
            String databaseName, String serviceName) {
        if (targetDbType == DbType.POSTGRESQL) {
            return "jdbc:postgresql://%s:%d/%s".formatted(hostname, port, databaseName);
        }
        if (targetDbType == DbType.ORACLE) {
            return "jdbc:oracle:thin:@%s:%d/%s".formatted(hostname, port, serviceName);
        }
        throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                "지원하지 않는 타겟 DB 유형입니다: " + targetDbType);
    }
}
