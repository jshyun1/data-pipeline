package com.company.pipeline.connector;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * kafka-connect/connectors/oracle-cdc-source.json.template과 동일한 필드 구성.
 * lob.enabled / skip.unparseable.ddl / event.processing.failure.handling.mode는
 * 실제 운영 중 DDL 파싱 크래시·LogMiner 재구성 실패를 겪고 나서 필수로 확인된 값이라
 * 항상 켜둔다(사용자 입력으로 끌 수 있게 하지 않음).
 */
@Component
public class DebeziumOracleTemplate {

    private static final String BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String COMMON_USER_PREFIX = "C##";

    /**
     * Oracle LogMiner는 CDB 레벨에서 동작하므로 반드시 공통 사용자(C##...)로 접속해야 한다.
     * PDB 로컬 사용자(예: appuser)로 시도하면 비밀번호가 맞아도 ORA-01017로 거부되는데,
     * 이때 Kafka Connect REST가 500 NPE로 응답하는 버그(AbstractHerder.maybeAddConfigErrors)가
     * 있어 원인 파악이 어려워진다 - 그래서 Kafka Connect에 보내기 전에 여기서 먼저 걸러낸다.
     */
    public static boolean isOracleCommonUser(String username) {
        return username != null && username.toUpperCase().startsWith(COMMON_USER_PREFIX);
    }

    public RenderedConnectorConfig render(SourceConnectorRequest request) {
        if (!isOracleCommonUser(request.username())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "Oracle CDC 소스는 공통 사용자(C##으로 시작하는 계정)만 사용할 수 있습니다. "
                            + "LogMiner는 CDB 레벨에서 동작해서 PDB 로컬 사용자(예: appuser)로는 인증할 수 없습니다. "
                            + "입력한 사용자명: " + request.username());
        }

        String connectorName = ConnectorNaming.sourceConnectorName(
                request.pipelineId(), "oracle", request.schema(), request.table());
        String tableIncludeList = request.schema() + "." + request.table();

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.oracle.OracleConnector");
        config.put("tasks.max", "1");
        config.put("database.hostname", request.hostname());
        config.put("database.port", String.valueOf(request.port()));
        config.put("database.user", request.username());
        config.put("database.password", request.password());
        config.put("database.dbname", "XE");
        config.put("database.pdb.name", request.serviceName());
        config.put("database.connection.adapter", "logminer");
        config.put("log.mining.strategy", "online_catalog");
        config.put("schema.history.internal.kafka.bootstrap.servers", BOOTSTRAP_SERVERS);
        // 커넥터마다 고유해야 함 - 공유하면 서로 다른 DB의 DDL 이력이 섞여 스키마 해석이 깨짐.
        config.put("schema.history.internal.kafka.topic", "schema-changes." + connectorName);
        config.put("topic.prefix", request.topicPrefix());
        config.put("schema.include.list", request.schema());
        config.put("table.include.list", tableIncludeList);
        config.put("tombstones.on.delete", "false");
        config.put("decimal.handling.mode", "double");
        config.put("include.schema.changes", "true");
        config.put("lob.enabled", "true");
        config.put("schema.history.internal.skip.unparseable.ddl", "true");
        config.put("event.processing.failure.handling.mode", "warn");

        return new RenderedConnectorConfig(connectorName, "SOURCE",
                "io.debezium.connector.oracle.OracleConnector", config);
    }
}
