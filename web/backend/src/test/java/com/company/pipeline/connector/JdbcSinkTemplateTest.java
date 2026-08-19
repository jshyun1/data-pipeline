package com.company.pipeline.connector;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SinkConnectorRequest;
import org.junit.jupiter.api.Test;

class JdbcSinkTemplateTest {

    private final JdbcSinkTemplate template = new JdbcSinkTemplate();

    // kafka-connect/connectors/postgres-cdc-sink.json.template과 동일 파라미터로 검증.
    @Test
    void render_postgresTarget_matchesExistingSinkTemplateShape() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                12L, DbType.POSTGRESQL, "target-db", 5432, "tarantula_app", "pw",
                "tarantula", null, "cdc_landing", "customers",
                DbType.ORACLE, "APPUSER", "CUSTOMERS", "oracle-cdc", true);

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.connectorName()).isEqualTo("sink-12-postgresql-cdc_landing-customers");
        var config = rendered.config();
        assertThat(config.get("connector.class")).isEqualTo("io.debezium.connector.jdbc.JdbcSinkConnector");
        assertThat(config.get("topics")).isEqualTo("oracle-cdc.APPUSER.CUSTOMERS");
        assertThat(config.get("connection.url")).isEqualTo("jdbc:postgresql://target-db:5432/tarantula");
        assertThat(config.get("insert.mode")).isEqualTo("upsert");
        assertThat(config.get("primary.key.mode")).isEqualTo("record_key");
        assertThat(config.get("schema.evolution")).isEqualTo("basic");
        assertThat(config.get("table.name.format")).isEqualTo("cdc_landing.customers");
        assertThat(config.get("delete.enabled")).isEqualTo("true");
        assertThat(config.get("errors.deadletterqueue.topic.name")).isEqualTo("dlq.pipeline-12");
        assertThat(config.get("errors.deadletterqueue.context.headers.enable")).isEqualTo("true");
        assertThat(config.get("errors.log.include.messages")).isEqualTo("false");
    }

    // 실제 운영에서 발견된 버그 재현: pipeline_definition에 sourceTable이 소문자로
    // 저장돼 있어도(사용자가 화면에 소문자로 입력한 경우), Debezium이 Oracle 소스에서
    // 실제로 만드는 토픽은 항상 대문자다 - 여기서 안 맞추면 싱크가 존재하지 않는 토픽을
    // 구독하게 되어 데이터가 조용히 안 흐른다.
    @Test
    void render_oracleSourceWithLowercaseStoredNames_upperCasesTopicToMatchDebezium() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                13L, DbType.POSTGRESQL, "target-db", 5432, "tarantula_app", "pw",
                "tarantula", null, "cdc_landing", "customers",
                DbType.ORACLE, "appuser", "customers", "oracle-cdc", true);

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.config().get("topics")).isEqualTo("oracle-cdc.APPUSER.CUSTOMERS");
    }

    @Test
    void render_oracleTarget_buildsOracleThinConnectionUrl() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                5L, DbType.ORACLE, "oracle-db", 1521, "appuser", "pw",
                null, "XEPDB1", "APPUSER", "CUSTOMERS_FROM_PG",
                DbType.POSTGRESQL, "cdc_landing", "customers", "postgres-cdc", true);

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.config().get("connection.url"))
                .isEqualTo("jdbc:oracle:thin:@oracle-db:1521/XEPDB1");
        assertThat(rendered.config().get("table.name.format")).isEqualTo("APPUSER.CUSTOMERS_FROM_PG");
    }
}
