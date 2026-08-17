package com.company.pipeline.heartbeat;

import com.company.pipeline.heartbeat.HeartbeatComponentRegistry.Component;
import java.time.OffsetDateTime;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 수집기 결측 감시 (U5). watchdogScheduler(전용 단일 스레드) 60초 주기로 각 수집기의
 * last_beat_at 을 보고, 실효 임계(max(expected, observed) x 3)를 넘으면 collector_outage 를 열고
 * 다시 beat 하면 닫는다. 보존 정리가 수백만 행을 도는 동안에도 이 감시는 살아 있어야 하므로
 * 배치 풀과 분리된 전용 스레드에서 돈다.
 */
@Service
public class WatchdogService {

    private static final Logger log = LoggerFactory.getLogger(WatchdogService.class);

    private final JdbcTemplate jdbc;

    public WatchdogService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @Scheduled(fixedRate = 60_000, initialDelay = 60_000, scheduler = "watchdogScheduler")
    public void sweep() {
        for (Component c : HeartbeatComponentRegistry.COMPONENTS) {
            try {
                checkOne(c);
            } catch (Exception ex) {
                log.warn("워치독 {} 점검 실패: {}", c.key(), ex.getMessage());
            }
        }
    }

    private void checkOne(Component c) {
        OffsetDateTime lastBeat = jdbc.queryForObject(
                "SELECT last_beat_at FROM system_heartbeat WHERE component_key = ?",
                OffsetDateTime.class, c.key());
        Integer observed = jdbc.queryForObject(
                "SELECT observed_interval_seconds FROM system_heartbeat WHERE component_key = ?",
                Integer.class, c.key());
        if (lastBeat == null) {
            return;   // 한 번도 안 뜬 컴포넌트는 자가진단이 DOWN 으로 표시(결측 구간은 beat 이력이 있어야 의미)
        }
        long baseline = Math.max(c.expectedIntervalSeconds(), observed == null ? 0 : observed);
        long threshold = baseline * 3;
        long ageSec = java.time.Duration.between(lastBeat, OffsetDateTime.now()).getSeconds();

        boolean hasOpen = countOpenOutage(c.key()) > 0;
        if (ageSec > threshold && !hasOpen) {
            jdbc.update("""
                    INSERT INTO collector_outage (component_key, started_at, detected_by, reason)
                    VALUES (?, ?, 'GAP', ?)
                    """, c.key(), lastBeat, "주기 3회 이상 이탈(" + ageSec + "초 무응답)");
            log.warn("워치독: {} 결측 감지({}초 무응답, 임계 {}초) - collector_outage GAP 개시", c.key(), ageSec, threshold);
        } else if (ageSec <= baseline && hasOpen) {
            jdbc.update("""
                    UPDATE collector_outage SET ended_at = now()
                    WHERE component_key = ? AND ended_at IS NULL
                    """, c.key());
            log.info("워치독: {} 수집 재개 - 결측 구간 종료", c.key());
        }
    }

    private int countOpenOutage(String key) {
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM collector_outage WHERE component_key = ? AND ended_at IS NULL",
                Integer.class, key);
        return n == null ? 0 : n;
    }
}
