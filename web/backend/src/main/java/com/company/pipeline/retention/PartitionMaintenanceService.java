package com.company.pipeline.retention;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 파티션 사전 생성(유지잡, 설계서 파티션 유지). RANGE 파티션 테이블에 "미래" 월 파티션을 미리 만들어
 * 두어, 새 데이터가 DEFAULT 로 몰리지 않고 월 파티션에 떨어지게 한다 → RetentionService 의
 * PARTITION_DROP 이 유효해진다({@link RetentionService}).
 *
 * <p>안전 원칙: 현재/과거 월은 이미 DEFAULT 에 행이 있어 파티션 생성이 겹침 오류를 낸다. 그래서 오직
 * "다음 달부터" 앞으로만 만든다(미래 구간엔 DEFAULT 행이 없다). 기존 DEFAULT 백로그의 재분배(일회성
 * 이관)는 이 잡의 책임이 아니다 - 별도 마이그레이션으로 처리한다.
 */
@Service
public class PartitionMaintenanceService {

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenanceService.class);
    private static final DateTimeFormatter YM = DateTimeFormatter.ofPattern("yyyyMM");
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_LOCAL_DATE;

    // 관리 대상 RANGE 파티션 테이블(스키마 고정). 파티션 키는 각 테이블의 timestamp 컬럼.
    private static final List<String> MANAGED = List.of(
            "infra_resource_sample", "pipeline_metric_snapshot");
    private static final int MONTHS_AHEAD = 3;

    private final JdbcTemplate jdbc;

    public PartitionMaintenanceService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    /** 하루 1회면 충분하나 여유로 6시간마다. 기동 45초 뒤 한 번 돌아 앞으로의 월 파티션을 확보한다. */
    @Scheduled(fixedRate = 21_600_000, initialDelay = 45_000, scheduler = "batchScheduler")
    public void ensureFuturePartitions() {
        LocalDate firstOfNextMonth = LocalDate.now(ZoneId.systemDefault())
                .withDayOfMonth(1).plusMonths(1);
        int created = 0;
        for (String table : MANAGED) {
            if (!isPartitioned(table)) {
                continue;   // 아직 파티션 전환 전인 테이블은 건너뛴다.
            }
            for (int m = 0; m < MONTHS_AHEAD; m++) {
                LocalDate start = firstOfNextMonth.plusMonths(m);
                LocalDate end = start.plusMonths(1);
                String part = table + "_p" + start.format(YM);
                try {
                    // IF NOT EXISTS: 이미 있으면 무해. 미래 구간이라 DEFAULT 와 겹치지 않는다.
                    jdbc.execute(String.format(
                            "CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
                            part, table, start.format(ISO), end.format(ISO)));
                    created++;
                } catch (Exception ex) {
                    log.warn("파티션 생성 실패 {}: {}", part, ex.getMessage());
                }
            }
        }
        log.info("파티션 유지잡 완료: {} 테이블, {}부터 {}개월 확보(create-if-not-exists {}건)",
                MANAGED.size(), firstOfNextMonth.format(ISO), MONTHS_AHEAD, created);
    }

    private boolean isPartitioned(String table) {
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM pg_partitioned_table pt "
                        + "JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = ?",
                Integer.class, table);
        return n != null && n > 0;
    }
}
