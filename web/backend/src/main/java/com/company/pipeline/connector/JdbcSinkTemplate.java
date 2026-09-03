package com.company.pipeline.connector;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.LogSinkConnectorRequest;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SinkConnectorRequest;
import com.company.pipeline.pipeline.PipelineLoadMode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * docs/kafka-webservice-design.md §11.3, §11.4. Debezium JDBC Sink는 Postgres/Oracle/MySQL
 * 드라이버가 플러그인 디렉터리에 번들돼 있어(kafka-connect/Dockerfile 참고 - 각
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

        PipelineLoadMode loadMode = PipelineLoadMode.from(request.loadMode());
        if (loadMode.isDelta()) {
            return renderDelta(request, loadMode, connectorName, topics);
        }

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
     * 델타 적재 싱크. DELTA_APPEND 는 변경 이벤트 한 건 = 델타 테이블 한 행, DELTA_UPSERT 는
     * PK당 한 행(마지막 상태 + 마지막 작업 종류).
     *
     * <p>Debezium JDBC 싱크는 CDC 봉투(before/after/op)를 스스로 풀지만 op 를 컬럼으로 남기지는
     * 못한다. 그래서 싱크 쪽에 ExtractNewRecordState SMT 를 걸어 after(삭제는 before) 를
     * 평면화하면서 op 필드를 구분컬럼 이름으로 붙인다(add.fields 의 {@code op:이름} 별칭 문법,
     * 접두어 {@code __} 는 비움). 소스 토픽의 메시지 형식은 그대로라 같은 토픽에 기존 upsert
     * 싱크를 나란히 붙일 수 있다.
     *
     * <ul>
     *   <li>{@code delete.tombstone.handling.mode=rewrite}: 기본값(tombstone)이면 delete 가
     *       빈 레코드로 바뀌어 사라진다. rewrite 여야 삭제 직전 값 + {@code __deleted=true} 로 남는다.
     *       {@code __deleted} 는 요구 모양(구분컬럼 1 + 소스 컬럼 N)에 없는 열이라 ReplaceField 로 뺀다.</li>
     *   <li>DELTA_APPEND: {@code insert.mode=insert}/{@code primary.key.mode=none} - 같은 PK 가 여러
     *       행으로 쌓여야 하므로 upsert·PK 를 쓰지 않는다.</li>
     *   <li>DELTA_UPSERT: {@code insert.mode=upsert}/{@code primary.key.mode=record_key} - 같은 PK 는
     *       덮어쓴다. delete 도 행 삭제가 아니라 구분값 d 로 덮어쓰므로 delete.enabled 는 여전히 false.
     *       순번(INSERT 때만 채번)은 덮어써도 안 바뀌어 "가져간 만큼 삭제" 기준이 못 되므로, 이벤트
     *       시각 {@code ts_ms} 를 {@value PipelineLoadMode#DELTA_TS_COLUMN} 컬럼으로 같이 넣는다.</li>
     *   <li>op 값은 Debezium 코드 그대로(c/u/d/r). 문자열로 바꿔 주는 내장 SMT 는 없다.</li>
     * </ul>
     * 구분컬럼을 맨 앞에 두기 위한 테이블 뼈대(APPEND)나 PK 검증(UPSERT)은 DeltaTargetTableService 가
     * 배포 시 먼저 한다.
     */
    private RenderedConnectorConfig renderDelta(SinkConnectorRequest request, PipelineLoadMode loadMode,
            String connectorName, String topics) {
        String opColumn = PipelineLoadMode.normalizeDeltaOpColumn(request.deltaOpColumn());
        boolean upsert = loadMode == PipelineLoadMode.DELTA_UPSERT;

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector");
        config.put("tasks.max", "1");
        config.put("topics", topics);
        config.put("connection.url", connectionUrl(request));
        config.put("connection.username", request.username());
        config.put("connection.password", request.password());
        config.put("insert.mode", upsert ? "upsert" : "insert");
        config.put("primary.key.mode", upsert ? "record_key" : "none");
        config.put("schema.evolution", "basic");
        config.put("table.name.format", request.targetSchema() + "." + request.targetTable());
        config.put("delete.enabled", "false");
        config.put("transforms", "unwrap,dropDeleted");
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.add.fields", upsert
                ? "op:" + opColumn + ",ts_ms:" + PipelineLoadMode.DELTA_TS_COLUMN
                : "op:" + opColumn);
        config.put("transforms.unwrap.add.fields.prefix", "");
        config.put("transforms.unwrap.delete.tombstone.handling.mode", "rewrite");
        config.put("transforms.dropDeleted.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        config.put("transforms.dropDeleted.exclude", "__deleted");
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
        if (targetDbType == DbType.MYSQL) {
            return "jdbc:mysql://%s:%d/%s".formatted(hostname, port, databaseName);
        }
        throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                "지원하지 않는 타겟 DB 유형입니다: " + targetDbType);
    }
}
