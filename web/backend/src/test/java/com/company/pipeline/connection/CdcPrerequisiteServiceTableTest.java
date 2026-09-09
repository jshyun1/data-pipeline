package com.company.pipeline.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.connection.dto.CdcTableReadinessResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 테이블 단위 CDC 조건 점검. Oracle 보충 로깅이 빠지면 UPDATE 의 PK 가 유실되는데
 * INSERT/DELETE 는 멀쩡해서 늦게 드러나므로, 판정이 정확한지 여기서 못 박는다.
 */
@ExtendWith(MockitoExtension.class)
class CdcPrerequisiteServiceTableTest {

    @Mock
    private ConnectionRepository connectionRepository;
    @Mock
    private SchemaDiscoveryService schemaDiscoveryService;

    private CdcTableReadinessResponse check(DbType dbType, List<String> logGroupTypes,
            String replicaIdentity, boolean dbLevelAllLogging) throws SQLException {
        PipelineConnection saved = new PipelineConnection(
                "conn", dbType, "host", 1521, "db", "svc", null, "C##DBZUSER", "cipher", null);
        when(connectionRepository.findById(7L)).thenReturn(Optional.of(saved));

        Connection jdbc = mock(Connection.class);
        when(schemaDiscoveryService.open(saved)).thenReturn(jdbc);

        // V$DATABASE 의 SUPPLEMENTAL_LOG_DATA_ALL (Statement 경로)
        Statement plain = mock(Statement.class);
        ResultSet plainRs = mock(ResultSet.class);
        lenient().when(jdbc.createStatement()).thenReturn(plain);
        lenient().when(plain.executeQuery(anyString())).thenReturn(plainRs);
        lenient().when(plainRs.next()).thenReturn(true);
        lenient().when(plainRs.getString(1)).thenReturn(dbLevelAllLogging ? "YES" : "NO");

        // ALL_LOG_GROUPS / pg_class (PreparedStatement 경로)
        PreparedStatement prepared = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        lenient().when(jdbc.prepareStatement(anyString())).thenReturn(prepared);
        lenient().when(prepared.executeQuery()).thenReturn(rs);
        lenient().doNothing().when(prepared).setQueryTimeout(anyInt());
        lenient().doNothing().when(prepared).setString(anyInt(), any());

        if (dbType == DbType.ORACLE) {
            Boolean[] hasNext = new Boolean[logGroupTypes.size() + 1];
            for (int i = 0; i < logGroupTypes.size(); i++) hasNext[i] = true;
            hasNext[logGroupTypes.size()] = false;
            lenient().when(rs.next()).thenReturn(hasNext[0],
                    java.util.Arrays.copyOfRange(hasNext, 1, hasNext.length));
            if (!logGroupTypes.isEmpty()) {
                lenient().when(rs.getString(1)).thenReturn(logGroupTypes.get(0),
                        logGroupTypes.subList(1, logGroupTypes.size()).toArray(new String[0]));
            }
        } else {
            lenient().when(rs.next()).thenReturn(true);
            lenient().when(rs.getString(1)).thenReturn(replicaIdentity);
        }

        CdcPrerequisiteService service =
                new CdcPrerequisiteService(connectionRepository, schemaDiscoveryService);
        return service.checkTable(7L, dbType == DbType.ORACLE ? "CSB" : "public",
                dbType == DbType.ORACLE ? "INS_PYM_STT" : "ins_pym_stt");
    }

    @Test
    void oracle_withAllColumnLogging_passes() throws Exception {
        var result = check(DbType.ORACLE, List.of("ALL COLUMN LOGGING"), null, false);
        assertThat(result.status()).isEqualTo("PASS");
        assertThat(result.actualValue()).isEqualTo("ALL COLUMN LOGGING");
    }

    @Test
    void oracle_withNoLogGroup_failsWithAlterStatement() throws Exception {
        var result = check(DbType.ORACLE, List.of(), null, false);
        assertThat(result.status()).isEqualTo("FAIL");
        assertThat(result.actualValue()).isEqualTo("설정 없음");
        assertThat(result.guidance())
                .contains("ALTER TABLE CSB.INS_PYM_STT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;");
    }

    // PK 로그 그룹만 있으면 UPDATE 의 PK 는 오지만 나머지 컬럼이 NULL 이라 여전히 부족하다.
    @Test
    void oracle_withOnlyPrimaryKeyLogging_stillFails() throws Exception {
        var result = check(DbType.ORACLE, List.of("PRIMARY KEY LOGGING"), null, false);
        assertThat(result.status()).isEqualTo("FAIL");
        assertThat(result.actualValue()).isEqualTo("PRIMARY KEY LOGGING");
    }

    // DB 전체에 ALL 보충 로깅이 걸려 있으면 테이블별 로그 그룹이 없어도 통과해야 한다.
    @Test
    void oracle_withDatabaseWideAllLogging_passesWithoutTableLogGroup() throws Exception {
        var result = check(DbType.ORACLE, List.of(), null, true);
        assertThat(result.status()).isEqualTo("PASS");
        assertThat(result.actualValue()).isEqualTo("데이터베이스 전체 ALL 보충 로깅");
    }

    @Test
    void postgres_replicaIdentityFull_passes() throws Exception {
        var result = check(DbType.POSTGRESQL, List.of(), "FULL", false);
        assertThat(result.status()).isEqualTo("PASS");
    }

    // 미러링에는 지장이 없어 WARN 까지만 - 배포를 막지는 않는다.
    @Test
    void postgres_replicaIdentityDefault_warnsOnly() throws Exception {
        var result = check(DbType.POSTGRESQL, List.of(), "DEFAULT", false);
        assertThat(result.status()).isEqualTo("WARN");
        assertThat(result.guidance()).contains("REPLICA IDENTITY FULL");
    }

    @Test
    void mysql_hasNoTableLevelCondition() throws Exception {
        var result = check(DbType.MYSQL, List.of(), null, false);
        assertThat(result.status()).isEqualTo("PASS");
        assertThat(result.guidance()).contains("binlog_row_image");
    }
}
