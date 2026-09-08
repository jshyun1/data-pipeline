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

    // 델타 적재: 변경 이벤트를 구분컬럼과 함께 append-only 로 쌓는 싱크. upsert/PK/delete 가 모두
    // 꺼지고, SMT 가 op 를 구분컬럼 이름으로 붙이며 delete 는 rewrite 로 살려야 한다.
    @Test
    void render_deltaAppend_usesInsertOnlySinkWithUnwrapTransform() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                21L, DbType.MYSQL, "mysql-db", 3306, "appuser", "pw",
                "warehouse", null, "warehouse", "aa_table_delta",
                DbType.ORACLE, "APPUSER", "AA_TABLE", "oracle-cdc", true,
                "DELTA_APPEND", "cdc_op");

        var config = template.render(request).config();

        assertThat(config.get("topics")).isEqualTo("oracle-cdc.APPUSER.AA_TABLE");
        assertThat(config.get("insert.mode")).isEqualTo("insert");
        assertThat(config.get("primary.key.mode")).isEqualTo("none");
        assertThat(config.get("delete.enabled")).isEqualTo("false");
        assertThat(config.get("schema.evolution")).isEqualTo("basic");
        assertThat(config.get("table.name.format")).isEqualTo("warehouse.aa_table_delta");
        assertThat(config.get("transforms")).isEqualTo("unwrap,dropDeleted");
        assertThat(config.get("transforms.unwrap.type")).isEqualTo("io.debezium.transforms.ExtractNewRecordState");
        assertThat(config.get("transforms.unwrap.add.fields")).isEqualTo("op:cdc_op");
        assertThat(config.get("transforms.unwrap.add.fields.prefix")).isEqualTo("");
        assertThat(config.get("transforms.unwrap.delete.tombstone.handling.mode")).isEqualTo("rewrite");
        assertThat(config.get("transforms.dropDeleted.exclude")).isEqualTo("__deleted");
        assertThat(config.get("errors.deadletterqueue.topic.name")).isEqualTo("dlq.pipeline-21");
    }

    // 델타 최신상태: PK당 한 행. upsert + record_key 로 덮어쓰되 delete 는 행 삭제가 아니라 d 로
    // 남기고, "가져간 만큼 삭제" 기준이 되는 이벤트 시각(ts_ms)을 cdc_ts 로 같이 넣는다.
    @Test
    void render_deltaUpsert_usesUpsertByRecordKeyWithTimestampField() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                24L, DbType.ORACLE, "ora", 1521, "u", "pw", null, "XEPDB1", "APPUSER", "AA_TABLE_DELTA",
                DbType.POSTGRESQL, "public", "aa_table", "postgres-cdc", true, "DELTA_UPSERT", "cdc_op");

        var config = template.render(request).config();

        assertThat(config.get("insert.mode")).isEqualTo("upsert");
        assertThat(config.get("primary.key.mode")).isEqualTo("record_key");
        assertThat(config.get("delete.enabled")).isEqualTo("false");
        assertThat(config.get("transforms.unwrap.add.fields")).isEqualTo("op:cdc_op,ts_ms:cdc_ts");
        assertThat(config.get("transforms.unwrap.delete.tombstone.handling.mode")).isEqualTo("rewrite");
        assertThat(config.get("transforms.dropDeleted.exclude")).isEqualTo("__deleted");
    }

    @Test
    void render_deltaAppend_blankOpColumnFallsBackToDefault() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                22L, DbType.POSTGRESQL, "pg", 5432, "u", "pw", "db", null, "public", "t_delta",
                DbType.MYSQL, "src", "t", "mysql-cdc", false, "delta_append", " ");

        assertThat(template.render(request).config().get("transforms.unwrap.add.fields")).isEqualTo("op:cdc_op");
    }

    @Test
    void render_upsertDefault_hasNoTransforms() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                23L, DbType.POSTGRESQL, "pg", 5432, "u", "pw", "db", null, "public", "t",
                DbType.MYSQL, "src", "t", "mysql-cdc", true);

        var config = template.render(request).config();
        assertThat(config.get("insert.mode")).isEqualTo("upsert");
        assertThat(config.containsKey("transforms")).isFalse();
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

    @Test
    void render_mysqlTarget_buildsMysqlConnectionUrl() {
        SinkConnectorRequest request = new SinkConnectorRequest(
                6L, DbType.MYSQL, "mysql-db", 3306, "appuser", "pw",
                "warehouse", null, "cdc_landing", "customers_from_pg",
                DbType.POSTGRESQL, "public", "customers", "postgres-cdc", true);

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.connectorName()).isEqualTo("sink-6-mysql-cdc_landing-customers_from_pg");
        assertThat(rendered.config().get("connection.url"))
                .isEqualTo("jdbc:mysql://mysql-db:3306/warehouse");
        assertThat(rendered.config().get("table.name.format")).isEqualTo("cdc_landing.customers_from_pg");
    }
}
