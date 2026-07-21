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
                "APPUSER", "CUSTOMERS", "oracle-cdc", true);

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
    }

    @Test
    void render_oracleTarget_buildsOracleThinConnectionUrl() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                5L, DbType.ORACLE, "oracle-db", 1521, "appuser", "pw",
                null, "XEPDB1", "APPUSER", "CUSTOMERS_FROM_PG",
                "cdc_landing", "customers", "postgres-cdc", true);

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.config().get("connection.url"))
                .isEqualTo("jdbc:oracle:thin:@oracle-db:1521/XEPDB1");
        assertThat(rendered.config().get("table.name.format")).isEqualTo("APPUSER.CUSTOMERS_FROM_PG");
    }
}
