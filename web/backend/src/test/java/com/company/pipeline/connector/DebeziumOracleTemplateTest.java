package com.company.pipeline.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import org.junit.jupiter.api.Test;

class DebeziumOracleTemplateTest {

    /** 기본값: 온라인 redo + 아카이브 (archive-log-only 꺼짐) */
    private final DebeziumOracleTemplate template =
            new DebeziumOracleTemplate(new OracleCdcProperties(null, null));

    // kafka-connect/connectors/oracle-cdc-source.json.template과 같은 파라미터로
    // 렌더링해서 필드값이 일치하는지 확인 (실제 운영 중인 커넥터와의 정합성 검증).
    @Test
    void render_matchesExistingOracleCdcSourceTemplateShape() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                12L, DbType.ORACLE, "oracle-db", 1521, "c##dbzuser", "ChangeMe_Cdc_2026!",
                null, "XEPDB1", "APPUSER", "CUSTOMERS", "oracle-cdc");

        RenderedConnectorConfig rendered = template.render(request);

        assertThat(rendered.connectorName()).isEqualTo("source-12-oracle-appuser-customers");
        assertThat(rendered.connectorClass()).isEqualTo("io.debezium.connector.oracle.OracleConnector");
        var config = rendered.config();
        assertThat(config.get("connector.class")).isEqualTo("io.debezium.connector.oracle.OracleConnector");
        assertThat(config.get("database.hostname")).isEqualTo("oracle-db");
        assertThat(config.get("database.port")).isEqualTo("1521");
        assertThat(config.get("database.dbname")).isEqualTo("XE");
        assertThat(config.get("database.pdb.name")).isEqualTo("XEPDB1");
        assertThat(config.get("database.connection.adapter")).isEqualTo("logminer");
        assertThat(config.get("log.mining.strategy")).isEqualTo("online_catalog");
        assertThat(config.get("log.mining.archive.log.only.mode")).isEqualTo("false");
        assertThat(config.get("topic.prefix")).isEqualTo("oracle-cdc");
        assertThat(config.get("schema.include.list")).isEqualTo("APPUSER");
        assertThat(config.get("table.include.list")).isEqualTo("APPUSER.CUSTOMERS");
        assertThat(config.get("lob.enabled")).isEqualTo("true");
        assertThat(config.get("schema.history.internal.skip.unparseable.ddl")).isEqualTo("true");
        assertThat(config.get("event.processing.failure.handling.mode")).isEqualTo("warn");
    }

    @Test
    void render_schemaHistoryTopic_isUniquePerConnector() {
        SourceConnectorRequest a = new SourceConnectorRequest(
                1L, DbType.ORACLE, "oracle-db", 1521, "c##u", "p", null, "XEPDB1", "APPUSER", "CUSTOMERS", "oracle-cdc");
        SourceConnectorRequest b = new SourceConnectorRequest(
                2L, DbType.ORACLE, "oracle-db", 1521, "c##u", "p", null, "XEPDB1", "APPUSER", "ORDERS", "oracle-cdc");

        String historyTopicA = (String) template.render(a).config().get("schema.history.internal.kafka.topic");
        String historyTopicB = (String) template.render(b).config().get("schema.history.internal.kafka.topic");

        assertThat(historyTopicA).isNotEqualTo(historyTopicB);
    }

    // Oracle LogMiner는 CDB 공통 사용자만 허용 - PDB 로컬 사용자(예: appuser)는 비밀번호가
    // 맞아도 ORA-01017로 거부되고 Kafka Connect가 그걸 500 NPE로 잘못 응답하는 버그가 있어서,
    // Kafka Connect에 보내기 전에 미리 걸러야 한다 (실제 배포 중 겪은 문제).
    @Test
    void render_rejectsNonCommonUser() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                1L, DbType.ORACLE, "oracle-db", 1521, "appuser", "p", null, "XEPDB1", "APPUSER", "CUSTOMERS", "oracle-cdc");

        assertThatThrownBy(() -> template.render(request))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("공통 사용자");
    }

    /**
     * 온라인 redo 접근이 막힌 환경에서 켜는 설정. 켜면 변경이 아카이브로 넘어간 뒤에야
     * 읽히므로 지연이 redo 스위치 주기만큼 늘어난다.
     */
    @Test
    void render_archiveLogOnlyMode_isDrivenByConfiguration() {
        var archiveOnly = new DebeziumOracleTemplate(new OracleCdcProperties(true, null));
        SourceConnectorRequest request = new SourceConnectorRequest(
                1L, DbType.ORACLE, "oracle-db", 1521, "c##u", "p", null, "XEPDB1", "APPUSER", "CUSTOMERS", "oracle-cdc");

        assertThat(archiveOnly.render(request).config().get("log.mining.archive.log.only.mode"))
                .isEqualTo("true");
    }

    /** CDB 이름은 XE 기본값이지만 다른 에디션을 붙일 수 있게 환경변수로 뺀다. */
    @Test
    void render_cdbName_fallsBackToXeAndIsOverridable() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                1L, DbType.ORACLE, "oracle-db", 1521, "c##u", "p", null, "XEPDB1", "APPUSER", "CUSTOMERS", "oracle-cdc");

        assertThat(template.render(request).config().get("database.dbname")).isEqualTo("XE");
        assertThat(new DebeziumOracleTemplate(new OracleCdcProperties(false, "ORCLCDB"))
                .render(request).config().get("database.dbname")).isEqualTo("ORCLCDB");
    }

    @Test
    void isOracleCommonUser_isCaseInsensitive() {
        assertThat(DebeziumOracleTemplate.isOracleCommonUser("c##dbzuser")).isTrue();
        assertThat(DebeziumOracleTemplate.isOracleCommonUser("C##DBZUSER")).isTrue();
        assertThat(DebeziumOracleTemplate.isOracleCommonUser("appuser")).isFalse();
        assertThat(DebeziumOracleTemplate.isOracleCommonUser(null)).isFalse();
    }

    @Test
    void render_mapsNoDataSnapshotMode() {
        SourceConnectorRequest request = new SourceConnectorRequest(
                3L, DbType.ORACLE, "host", 1521, "c##u", "p", null, "XEPDB1", "APP", "T", "ora-cdc", "NO_DATA");
        assertThat(template.render(request).config().get("snapshot.mode")).isEqualTo("no_data");
    }
}
