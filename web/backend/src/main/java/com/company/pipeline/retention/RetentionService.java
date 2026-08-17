package com.company.pipeline.retention;

import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 보존 정리 (U6, 설계서 4-3/C10). retention_policy 를 읽어 매시간 정리한다.
 *
 * <p>배치 상한을 "횟수"가 아니라 "시간 예산(기본 20분)"으로 잡고 잔량 0까지 반복한다 - 고정 횟수
 * 상한은 유입이 더 크면 backlog 를 쌓으면서도 자가진단이 초록으로 뜨는 함정이 있다. 실행 주기도
 * 일 1회가 아니라 매시간(24배 여유). 배치마다 독립 트랜잭션(JdbcTemplate 자동커밋)이라 한 배치가
 * 실패해도 이전 배치는 커밋돼 있다.
 *
 * <p>정리 가능한 테이블은 <b>코드 화이트리스트</b>가 원천이다(table_name/time_column 은 동적 SQL
 * 조립이라 바인딩할 수 없다 - quote_ident 로 감싸고 화이트리스트 밖 이름은 무시한다).
 *
 * <p>PARTITION_DROP 모드(pipeline_metric_snapshot)는 V27 파티션 전환 이후에만 유효하다. 아직
 * 전환 전이면(일반 테이블) 이 모드를 건너뛰고 backlog 로 남긴다 - 일반 테이블에 DROP PARTITION 을
 * 시도해 매시간 예외를 내지 않기 위해서다.
 */
@Service
public class RetentionService {

    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

    // 정리 대상 화이트리스트(retention_policy 시드와 일치). 이 목록 밖 table_name 은 무시한다.
    private static final Set<String> ALLOWED_TABLES = Set.of(
            "pipeline_metric_snapshot", "nifi_execution_log", "nifi_processor_run",
            "pipeline_command_history", "collector_outage",
            // V26+ 신규 보존 대상
            "pipeline_load_rollup", "alert_signal_sample", "alert_instance",
            "notification_delivery", "infra_resource_sample", "infra_resource_rollup");

    private final JdbcTemplate jdbc;
    private final SettingService settings;

    public RetentionService(DataSource dataSource, SettingService settings) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.settings = settings;
    }

    @Scheduled(fixedRate = 3_600_000, initialDelay = 300_000, scheduler = "batchScheduler")
    public void sweep() {
        runOnce();
    }

    /** 전 정책을 한 바퀴 정리한다(스케줄러/수동 트리거 공용). 시간 예산을 넘기면 다음 주기로 넘긴다. */
    public void runOnce() {
        long budgetMs = settings.getInt(SettingKey.RETENTION_RUN_BUDGET_MINUTES) * 60_000L;
        long deadline = System.currentTimeMillis() + budgetMs;

        List<Map<String, Object>> policies = jdbc.queryForList(
                "SELECT table_name, time_column, retention_days, batch_rows, purge_mode "
                        + "FROM retention_policy WHERE enabled = true ORDER BY table_name");
        for (Map<String, Object> p : policies) {
            String table = (String) p.get("table_name");
            if (!ALLOWED_TABLES.contains(table)) {
                recordError(table, "화이트리스트 밖 테이블(코드가 원천) - 무시");
                continue;
            }
            if (System.currentTimeMillis() >= deadline) {
                log.warn("보존 정리 시간 예산 초과 - 남은 정책은 다음 주기로 이월");
                break;
            }
            try {
                purgeOne(p, deadline);
            } catch (Exception ex) {
                log.warn("보존 정리 실패({}): {}", table, ex.getMessage());
                recordError(table, ex.getMessage());
            }
        }
    }

    private void purgeOne(Map<String, Object> p, long deadline) {
        String table = (String) p.get("table_name");
        String timeCol = (String) p.get("time_column");
        int retentionDays = ((Number) p.get("retention_days")).intValue();
        int batchRows = ((Number) p.get("batch_rows")).intValue();
        String purgeMode = (String) p.get("purge_mode");
        OffsetDateTime cutoff = OffsetDateTime.now().minusDays(retentionDays);
        long started = System.currentTimeMillis();

        if ("PARTITION_DROP".equals(purgeMode) && !isPartitioned(table)) {
            // V27 파티션 전환 전이라 일반 테이블이다. DROP PARTITION 을 시도하지 않고 남은 초과분을
            // backlog 로만 기록한다(전환 후 이 분기는 파티션 드롭으로 바뀐다).
            long backlog = countOlderThan(table, timeCol, cutoff);
            jdbc.update("""
                    UPDATE retention_policy SET last_run_at = now(), last_deleted_rows = 0,
                        last_duration_ms = ?, backlog_rows = ?,
                        last_error = 'PARTITION_DROP 대기(V27 파티션 전환 전) - 정리 보류', updated_at = now()
                    WHERE table_name = ?
                    """, System.currentTimeMillis() - started, backlog, table);
            return;
        }

        // DELETE_BATCH: ctid 서브쿼리로 배치 삭제, 잔량 0까지(또는 시간 예산까지) 반복.
        String qTable = quoteIdent(table);
        String qCol = quoteIdent(timeCol);
        String deleteSql = "DELETE FROM " + qTable + " WHERE ctid IN ("
                + "SELECT ctid FROM " + qTable + " WHERE " + qCol + " < ? LIMIT " + batchRows + ")";
        long totalDeleted = 0;
        while (System.currentTimeMillis() < deadline) {
            int deleted = jdbc.update(deleteSql, cutoff);
            totalDeleted += deleted;
            if (deleted < batchRows) {
                break;   // 잔량 소진
            }
        }
        long backlog = countOlderThan(table, timeCol, cutoff);
        jdbc.update("""
                UPDATE retention_policy SET last_run_at = now(), last_deleted_rows = ?,
                    last_duration_ms = ?, backlog_rows = ?, last_error = NULL, updated_at = now()
                WHERE table_name = ?
                """, totalDeleted, System.currentTimeMillis() - started, backlog, table);
        if (backlog > 0) {
            log.warn("보존 정리 {}: {}행 삭제했으나 잔량 {}행 - 정리가 유입을 못 따라감", table, totalDeleted, backlog);
        }
    }

    private boolean isPartitioned(String table) {
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM pg_partitioned_table pt "
                        + "JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = ?",
                Integer.class, table);
        return n != null && n > 0;
    }

    private long countOlderThan(String table, String timeCol, OffsetDateTime cutoff) {
        Long n = jdbc.queryForObject(
                "SELECT count(*) FROM " + quoteIdent(table) + " WHERE " + quoteIdent(timeCol) + " < ?",
                Long.class, cutoff);
        return n == null ? 0 : n;
    }

    private void recordError(String table, String error) {
        try {
            jdbc.update("UPDATE retention_policy SET last_error = ?, updated_at = now() WHERE table_name = ?",
                    error, table);
        } catch (Exception ignored) {
            // best-effort
        }
    }

    /** 식별자를 안전하게 인용한다(동적 SQL 조립 - 값이 아니라 이름이라 바인딩 불가). */
    private String quoteIdent(String identifier) {
        return jdbc.queryForObject("SELECT quote_ident(?)", String.class, identifier);
    }
}
