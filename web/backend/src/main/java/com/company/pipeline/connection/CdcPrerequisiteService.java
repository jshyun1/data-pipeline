package com.company.pipeline.connection;

import com.company.pipeline.connection.dto.CdcPrerequisiteCheckResponse;
import com.company.pipeline.connection.dto.CdcPrerequisiteResponse;
import com.company.pipeline.connection.dto.CdcTableReadinessResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.springframework.stereotype.Service;

@Service
public class CdcPrerequisiteService {
    private final ConnectionRepository connectionRepository;
    private final SchemaDiscoveryService schemaDiscoveryService;

    public CdcPrerequisiteService(ConnectionRepository connectionRepository,
            SchemaDiscoveryService schemaDiscoveryService) {
        this.connectionRepository = connectionRepository;
        this.schemaDiscoveryService = schemaDiscoveryService;
    }

    public CdcPrerequisiteResponse check(Long connectionId) {
        PipelineConnection saved = connectionRepository.findById(connectionId)
                .orElseThrow(() -> new ConnectionNotFoundException(connectionId));
        List<CdcPrerequisiteCheckResponse> checks;
        try (Connection jdbc = schemaDiscoveryService.open(saved)) {
            checks = switch (saved.getDbType()) {
                case ORACLE -> checkOracle(jdbc, saved);
                case POSTGRESQL -> checkPostgres(jdbc);
                case MYSQL -> checkMysql(jdbc);
            };
        } catch (SQLException ex) {
            checks = List.of(new CdcPrerequisiteCheckResponse(
                    "CONNECTION", "데이터베이스 연결", "FAIL", null,
                    "연결정보와 네트워크를 확인하세요: " + safeMessage(ex)));
        }
        return new CdcPrerequisiteResponse(saved.getId(), saved.getDbType(), LocalDateTime.now(),
                overall(checks), checks);
    }

    /**
     * CDC 소스로 쓸 <b>개별 테이블</b>의 조건을 점검한다.
     *
     * <p><b>Oracle</b>: 테이블별 {@code SUPPLEMENTAL LOG DATA (ALL) COLUMNS} 가 없으면 UPDATE 의
     * redo 에 «바뀐 컬럼»만 남는다. 그러면 Debezium 이 PK 값을 읽지 못해 이벤트 키가 0(숫자 PK
     * 기준)으로 나가고, 싱크는 그 0을 키로 upsert 해서 엉뚱한 행 하나에 모든 UPDATE 를 뭉갠다.
     * INSERT·DELETE 는 redo 에 행 전체가 남아 정상이라 <b>겉보기엔 잘 도는 것처럼 보인다</b> -
     * 2026-09-05 운영에서 이 조건이 빠진 채 UPDATE 가 조용히 유실된 사례가 있어 FAIL 로 막는다.
     *
     * <p><b>PostgreSQL</b>: {@code REPLICA IDENTITY} 가 FULL 이 아니면 DELETE·UPDATE 의 before
     * 이미지에 PK 만 담긴다. 미러링(UPSERT)에는 지장이 없고 삭제 직전 값이 필요한 델타 적재에서만
     * 문제라 WARN 으로 둔다 - FULL 은 그 테이블의 WAL 량을 늘리므로 일괄 강제할 조건이 아니다.
     *
     * <p><b>MySQL</b>: {@code binlog_row_image} 가 서버 단위라 연결 점검에서 이미 본다.
     */
    public CdcTableReadinessResponse checkTable(Long connectionId, String schema, String table) {
        PipelineConnection saved = connectionRepository.findById(connectionId)
                .orElseThrow(() -> new ConnectionNotFoundException(connectionId));
        try (Connection jdbc = schemaDiscoveryService.open(saved)) {
            return switch (saved.getDbType()) {
                case ORACLE -> oracleTableCheck(jdbc, saved.getDbType(), schema, table);
                case POSTGRESQL -> postgresTableCheck(jdbc, saved.getDbType(), schema, table);
                case MYSQL -> new CdcTableReadinessResponse(schema, table, saved.getDbType(), "PASS",
                        "테이블 단위 조건 없음",
                        "MySQL은 binlog_row_image(서버 단위)만 만족하면 되며 연결 점검에서 확인합니다.");
            };
        } catch (SQLException ex) {
            return new CdcTableReadinessResponse(schema, table, saved.getDbType(), "UNKNOWN", null,
                    "테이블 조건을 조회하지 못했습니다: " + safeMessage(ex));
        }
    }

    private CdcTableReadinessResponse oracleTableCheck(Connection jdbc, DbType dbType,
            String schema, String table) {
        // DB 전체에 ALL 보충 로깅이 걸려 있으면 테이블별 로그 그룹이 없어도 모든 컬럼이 남는다.
        try {
            if ("YES".equalsIgnoreCase(scalar(jdbc, "SELECT SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE"))) {
                return new CdcTableReadinessResponse(schema, table, dbType, "PASS",
                        "데이터베이스 전체 ALL 보충 로깅", "테이블별 설정 없이도 모든 컬럼이 redo에 남습니다.");
            }
        } catch (SQLException ignored) {
            // V$DATABASE 조회 권한이 없을 수 있다 - 테이블 로그 그룹만 보고 판단한다.
        }

        List<String> groupTypes = new ArrayList<>();
        String sql = "SELECT log_group_type FROM all_log_groups WHERE owner = ? AND table_name = ?";
        try (PreparedStatement statement = jdbc.prepareStatement(sql)) {
            statement.setQueryTimeout(5);
            statement.setString(1, schema.toUpperCase(Locale.ROOT));
            statement.setString(2, table.toUpperCase(Locale.ROOT));
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) groupTypes.add(rs.getString(1));
            }
        } catch (SQLException ex) {
            return new CdcTableReadinessResponse(schema, table, dbType, "UNKNOWN", null,
                    "ALL_LOG_GROUPS 조회 권한을 확인하세요. (조회 실패: " + safeMessage(ex) + ")");
        }

        String fix = "ALTER TABLE " + schema.toUpperCase(Locale.ROOT) + "." + table.toUpperCase(Locale.ROOT)
                + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;";
        if (groupTypes.stream().anyMatch(type -> "ALL COLUMN LOGGING".equalsIgnoreCase(type))) {
            return new CdcTableReadinessResponse(schema, table, dbType, "PASS", "ALL COLUMN LOGGING", null);
        }
        return new CdcTableReadinessResponse(schema, table, dbType, "FAIL",
                groupTypes.isEmpty() ? "설정 없음" : String.join(", ", groupTypes),
                "이 테이블은 ALL COLUMNS 보충 로깅이 없어 UPDATE의 PK와 변경되지 않은 컬럼이 유실됩니다"
                        + "(INSERT/DELETE만 정상이라 문제가 늦게 드러납니다). 다음을 적용하세요: " + fix);
    }

    private CdcTableReadinessResponse postgresTableCheck(Connection jdbc, DbType dbType,
            String schema, String table) {
        String sql = "SELECT CASE c.relreplident WHEN 'f' THEN 'FULL' WHEN 'd' THEN 'DEFAULT' "
                + "WHEN 'i' THEN 'INDEX' WHEN 'n' THEN 'NOTHING' END "
                + "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "WHERE n.nspname = ? AND c.relname = ?";
        try (PreparedStatement statement = jdbc.prepareStatement(sql)) {
            statement.setQueryTimeout(5);
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet rs = statement.executeQuery()) {
                String identity = rs.next() ? rs.getString(1) : null;
                if ("FULL".equals(identity)) {
                    return new CdcTableReadinessResponse(schema, table, dbType, "PASS", identity, null);
                }
                return new CdcTableReadinessResponse(schema, table, dbType, "WARN", identity,
                        "REPLICA IDENTITY가 FULL이 아니라 DELETE 이벤트에 PK 외 컬럼이 NULL로 옵니다. "
                                + "삭제 직전 값이 필요한 델타 적재라면 ALTER TABLE " + schema + "." + table
                                + " REPLICA IDENTITY FULL; 을 적용하세요(해당 테이블 WAL 양이 늘어납니다).");
            }
        } catch (SQLException ex) {
            return new CdcTableReadinessResponse(schema, table, dbType, "UNKNOWN", null,
                    "pg_class 조회 권한을 확인하세요. (조회 실패: " + safeMessage(ex) + ")");
        }
    }

    private List<CdcPrerequisiteCheckResponse> checkOracle(Connection jdbc, PipelineConnection saved) {
        List<CdcPrerequisiteCheckResponse> checks = new ArrayList<>();
        checks.add(equalsCheck(jdbc, "ORACLE_ARCHIVELOG", "ARCHIVELOG 모드",
                "SELECT LOG_MODE FROM V$DATABASE", "ARCHIVELOG",
                "DBA에게 ARCHIVELOG 모드 활성화를 요청하세요."));
        checks.add(equalsCheck(jdbc, "ORACLE_SUPPLEMENTAL_MIN", "최소 보충 로깅",
                "SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE", "YES",
                "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA를 적용하세요."));
        boolean commonUser = saved.getUsername() != null
                && saved.getUsername().toUpperCase().startsWith("C##");
        checks.add(new CdcPrerequisiteCheckResponse(
                "ORACLE_COMMON_USER", "CDB 공통 사용자", commonUser ? "PASS" : "FAIL",
                saved.getUsername(), "Oracle LogMiner 소스 계정은 C##으로 시작하는 공통 사용자여야 합니다."));
        checks.add(countCheck(jdbc, "ORACLE_LOGMINER_PRIVILEGES", "LogMiner 핵심 권한",
                "SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE IN "
                        + "('LOGMINING','SELECT ANY TRANSACTION','SELECT ANY DICTIONARY')",
                3, "LOGMINING, SELECT ANY TRANSACTION, SELECT ANY DICTIONARY 권한을 확인하세요."));
        // 테이블별 조건이라 연결 단위로는 판정할 수 없다. 실제 판정은 checkTable() 이 하고,
        // 생성 마법사의 대상 선택 단계와 배포 시점이 그것을 호출한다.
        checks.add(new CdcPrerequisiteCheckResponse(
                "ORACLE_TABLE_SUPPLEMENTAL", "테이블별 보충 로깅", "WARN", "대상 선택 전",
                "테이블별 ALL COLUMNS 보충 로깅은 생성 마법사에서 선택한 테이블 기준으로 다시 점검합니다."));
        return checks;
    }

    private List<CdcPrerequisiteCheckResponse> checkMysql(Connection jdbc) {
        List<CdcPrerequisiteCheckResponse> checks = new ArrayList<>();
        checks.add(variableEqualsCheck(jdbc, "MYSQL_BINLOG_FORMAT", "binlog_format",
                "binlog_format", "ROW",
                "MySQL CDC는 binlog_format=ROW가 필요합니다."));
        checks.add(variableEqualsCheck(jdbc, "MYSQL_BINLOG_ROW_IMAGE", "binlog_row_image",
                "binlog_row_image", "FULL",
                "MySQL CDC는 변경 전/후 컬럼 캡처를 위해 binlog_row_image=FULL을 권장합니다."));
        checks.add(mysqlServerIdCheck(jdbc));
        checks.add(countCheck(jdbc, "MYSQL_REPLICATION_PRIVILEGES", "복제 권한",
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.USER_PRIVILEGES "
                        + "WHERE REPLACE(GRANTEE, '''', '') = CURRENT_USER() "
                        + "AND PRIVILEGE_TYPE IN ('REPLICATION SLAVE', 'REPLICATION CLIENT')",
                2, "CDC 계정에 REPLICATION SLAVE/REPLICA, REPLICATION CLIENT 권한을 부여하세요."));
        return checks;
    }

    private List<CdcPrerequisiteCheckResponse> checkPostgres(Connection jdbc) {
        List<CdcPrerequisiteCheckResponse> checks = new ArrayList<>();
        checks.add(equalsCheck(jdbc, "POSTGRES_WAL_LEVEL", "wal_level",
                "SHOW wal_level", "logical", "postgresql.conf의 wal_level을 logical로 설정하세요."));
        checks.add(equalsCheck(jdbc, "POSTGRES_REPLICATION_ROLE", "복제 권한",
                "SELECT rolreplication::text FROM pg_roles WHERE rolname = current_user", "true",
                "CDC 계정에 REPLICATION 권한을 부여하세요."));
        checks.add(slotCapacityCheck(jdbc));
        return checks;
    }

    private CdcPrerequisiteCheckResponse equalsCheck(Connection jdbc, String code, String label,
            String sql, String expected, String guidance) {
        try {
            String actual = scalar(jdbc, sql);
            boolean pass = expected.equalsIgnoreCase(actual);
            return new CdcPrerequisiteCheckResponse(code, label, pass ? "PASS" : "FAIL", actual, guidance);
        } catch (SQLException ex) {
            return unknown(code, label, ex, guidance);
        }
    }

    private CdcPrerequisiteCheckResponse countCheck(Connection jdbc, String code, String label,
            String sql, int expectedMinimum, String guidance) {
        try {
            String actual = scalar(jdbc, sql);
            boolean pass = Integer.parseInt(actual) >= expectedMinimum;
            return new CdcPrerequisiteCheckResponse(code, label, pass ? "PASS" : "FAIL",
                    actual + "/" + expectedMinimum, guidance);
        } catch (SQLException | NumberFormatException ex) {
            return unknown(code, label, ex, guidance);
        }
    }

    private CdcPrerequisiteCheckResponse variableEqualsCheck(Connection jdbc, String code, String label,
            String variableName, String expected, String guidance) {
        try {
            String actual = variableValue(jdbc, variableName);
            boolean pass = expected.equalsIgnoreCase(actual);
            return new CdcPrerequisiteCheckResponse(code, label, pass ? "PASS" : "FAIL", actual, guidance);
        } catch (SQLException ex) {
            return unknown(code, label, ex, guidance);
        }
    }

    private CdcPrerequisiteCheckResponse slotCapacityCheck(Connection jdbc) {
        String sql = "SELECT current_setting('max_replication_slots')::int, "
                + "(SELECT count(*) FROM pg_replication_slots)";
        try (Statement statement = jdbc.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) throw new SQLException("복제 슬롯 설정 결과가 없습니다.");
            int max = rs.getInt(1);
            int used = rs.getInt(2);
            return new CdcPrerequisiteCheckResponse("POSTGRES_SLOT_CAPACITY", "복제 슬롯 여유",
                    used < max ? "PASS" : "FAIL", used + "/" + max,
                    "max_replication_slots를 늘리거나 사용하지 않는 슬롯을 정리하세요.");
        } catch (SQLException ex) {
            return unknown("POSTGRES_SLOT_CAPACITY", "복제 슬롯 여유", ex,
                    "max_replication_slots와 pg_replication_slots 조회 권한을 확인하세요.");
        }
    }

    private CdcPrerequisiteCheckResponse mysqlServerIdCheck(Connection jdbc) {
        try {
            String actual = variableValue(jdbc, "server_id");
            boolean pass = actual != null && !"0".equals(actual);
            return new CdcPrerequisiteCheckResponse("MYSQL_SERVER_ID", "server_id",
                    pass ? "PASS" : "FAIL", actual,
                    "MySQL 인스턴스의 server_id를 0이 아닌 고유값으로 설정하세요.");
        } catch (SQLException ex) {
            return unknown("MYSQL_SERVER_ID", "server_id", ex,
                    "SHOW VARIABLES LIKE 'server_id' 조회 권한을 확인하세요.");
        }
    }

    private String scalar(Connection jdbc, String sql) throws SQLException {
        try (Statement statement = jdbc.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) throw new SQLException("점검 결과가 없습니다.");
            return rs.getString(1);
        }
    }

    private String variableValue(Connection jdbc, String name) throws SQLException {
        try (Statement statement = jdbc.createStatement();
                ResultSet rs = statement.executeQuery("SHOW VARIABLES LIKE '" + name + "'")) {
            if (!rs.next()) throw new SQLException("변수 조회 결과가 없습니다: " + name);
            return rs.getString(2);
        }
    }

    private CdcPrerequisiteCheckResponse unknown(String code, String label, Exception ex, String guidance) {
        return new CdcPrerequisiteCheckResponse(code, label, "UNKNOWN", null,
                guidance + " (조회 실패: " + safeMessage(ex) + ")");
    }

    private String overall(List<CdcPrerequisiteCheckResponse> checks) {
        if (checks.stream().anyMatch(check -> "FAIL".equals(check.status()))) return "FAIL";
        if (checks.stream().anyMatch(check -> !"PASS".equals(check.status()))) return "WARN";
        return "PASS";
    }

    private String safeMessage(Exception ex) {
        return ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    }
}
