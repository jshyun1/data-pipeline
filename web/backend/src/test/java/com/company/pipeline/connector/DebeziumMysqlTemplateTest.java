package com.company.pipeline.connector;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import org.junit.jupiter.api.Test;

class DebeziumMysqlTemplateTest {

    private final DebeziumMysqlTemplate template = new DebeziumMysqlTemplate();

    @Test
    void render_usesMysqlConnectorAndBinlogDatabaseTableFilters() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                7L, DbType.MYSQL, "mysql-db", 3306, "dbzuser", "pw",
                "appdb", null, "appdb", "customers", "mysql-cdc");

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.connectorName()).isEqualTo("source-7-mysql-appdb-customers");
        var config = rendered.config();
        assertThat(config.get("connector.class")).isEqualTo("io.debezium.connector.mysql.MySqlConnector");
        assertThat(config.get("database.hostname")).isEqualTo("mysql-db");
        assertThat(config.get("database.port")).isEqualTo("3306");
        assertThat(config.get("database.include.list")).isEqualTo("appdb");
        assertThat(config.get("table.include.list")).isEqualTo("appdb.customers");
        assertThat(config.get("topic.prefix")).isEqualTo("mysql-cdc");
        assertThat(config.get("schema.history.internal.kafka.topic"))
                .isEqualTo("schema-changes.source-7-mysql-appdb-customers");
        assertThat(config.get("database.server.id")).isEqualTo("54007");
        assertThat(config.get("snapshot.mode")).isEqualTo("initial");
    }

    @Test
    void render_mapsNoDataSnapshotMode() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                8L, DbType.MYSQL, "mysql-db", 3306, "dbzuser", "pw",
                "appdb", null, "appdb", "orders", "mysql-cdc", "NO_DATA");

        assertThat(template.render(request).config().get("snapshot.mode")).isEqualTo("no_data");
    }
}
