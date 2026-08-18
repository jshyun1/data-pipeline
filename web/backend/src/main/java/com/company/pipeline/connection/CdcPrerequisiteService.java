package com.company.pipeline.connection;

import com.company.pipeline.connection.dto.CdcPrerequisiteCheckResponse;
import com.company.pipeline.connection.dto.CdcPrerequisiteResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
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
            checks = saved.getDbType() == DbType.ORACLE ? checkOracle(jdbc, saved) : checkPostgres(jdbc);
        } catch (SQLException ex) {
            checks = List.of(new CdcPrerequisiteCheckResponse(
                    "CONNECTION", "데이터베이스 연결", "FAIL", null,
                    "연결정보와 네트워크를 확인하세요: " + safeMessage(ex)));
        }
        return new CdcPrerequisiteResponse(saved.getId(), saved.getDbType(), LocalDateTime.now(),
                overall(checks), checks);
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
        checks.add(new CdcPrerequisiteCheckResponse(
                "ORACLE_TABLE_SUPPLEMENTAL", "테이블별 보충 로깅", "WARN", "대상 선택 전",
                "테이블별 ALL COLUMNS 보충 로깅은 생성 마법사에서 선택한 테이블 기준으로 다시 점검합니다."));
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

    private String scalar(Connection jdbc, String sql) throws SQLException {
        try (Statement statement = jdbc.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) throw new SQLException("점검 결과가 없습니다.");
            return rs.getString(1);
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
