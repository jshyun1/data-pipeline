package com.company.pipeline.connector;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import org.junit.jupiter.api.Test;

class DebeziumPostgresTemplateTest {

    private final DebeziumPostgresTemplate template = new DebeziumPostgresTemplate();

    @Test
    void render_usesPgoutputAndSanitizedSlotName() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                7L, DbType.POSTGRESQL, "target-db", 5432, "tarantula_app", "pw",
                "tarantula", null, "cdc_landing", "customers", "postgres-cdc");

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.connectorName()).isEqualTo("source-7-postgres-cdc_landing-customers");
        var config = rendered.config();
        assertThat(config.get("connector.class")).isEqualTo("io.debezium.connector.postgresql.PostgresConnector");
        assertThat(config.get("plugin.name")).isEqualTo("pgoutput");
        assertThat(config.get("database.dbname")).isEqualTo("tarantula");
        assertThat(config.get("schema.include.list")).isEqualTo("cdc_landing");
        assertThat(config.get("table.include.list")).isEqualTo("cdc_landing.customers");
        // 슬롯/publication 이름은 소문자/숫자/밑줄만 허용되므로 하이픈이 없어야 함.
        assertThat((String) config.get("slot.name")).doesNotContain("-");
        assertThat(config.get("slot.name")).isEqualTo(config.get("publication.name"));
    }

    @Test
    void render_slotName_isUniquePerConnector() {
        SourceConnectorRequest a = new SourceConnectorRequest(
                1L, DbType.POSTGRESQL, "target-db", 5432, "u", "p", "db", null, "s", "t1", "pg-cdc");
        SourceConnectorRequest b = new SourceConnectorRequest(
                2L, DbType.POSTGRESQL, "target-db", 5432, "u", "p", "db", null, "s", "t2", "pg-cdc");

        assertThat(template.render(a).config().get("slot.name"))
                .isNotEqualTo(template.render(b).config().get("slot.name"));
    }
}
