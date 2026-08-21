package com.company.pipeline.heartbeat;

import com.company.pipeline.common.ApiResponse;
import java.lang.management.ManagementFactory;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 자가진단 (U5, 설계서 6-7-2). 지표 수집기 생존 + 시계 정합을 보고한다. 알림 경로·저장소·구성점검의
 * 나머지 항목은 이후 단위(U6 보존/U11~ 발송)에서 확장한다.
 *
 * <p>"모름"을 "정상"으로 칠하지 않는다(원칙 A): last_beat_at 이 없는데 기동 후 interval x 5 가
 * 지났으면 PENDING 이 아니라 DOWN 이다(한 번도 안 뜬 수집기가 회색으로 영원히 미탐되는 것 방지).
 */
@RestController
@RequestMapping("/api/admin/self-check")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.ADMIN)
public class SelfCheckController {

    private final JdbcTemplate jdbc;

    public SelfCheckController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public record CollectorStatus(String key, String label, String metricSource, String status,
                                  OffsetDateTime lastBeatAt, Long ageSeconds, int expectedIntervalSeconds,
                                  Integer observedIntervalSeconds, String lastResult, String lastError) {}

    public record SelfCheckResponse(OffsetDateTime checkedAt, List<CollectorStatus> collectors,
                                    long clockSkewSeconds) {}

    @GetMapping
    public ApiResponse<SelfCheckResponse> selfCheck() {
        long uptimeSec = (System.currentTimeMillis() - ManagementFactory.getRuntimeMXBean().getStartTime()) / 1000;
        List<CollectorStatus> collectors = HeartbeatComponentRegistry.COMPONENTS.stream()
                .map(c -> toStatus(c, uptimeSec))
                .toList();
        return ApiResponse.success(new SelfCheckResponse(OffsetDateTime.now(), collectors, clockSkewSeconds()));
    }

    private CollectorStatus toStatus(HeartbeatComponentRegistry.Component c, long uptimeSec) {
        OffsetDateTime lastBeat = queryTimestamp(
                "SELECT last_beat_at FROM system_heartbeat WHERE component_key = ?", c.key());
        Integer observed = queryInt(
                "SELECT observed_interval_seconds FROM system_heartbeat WHERE component_key = ?", c.key());
        String lastResult = queryString(
                "SELECT last_result FROM system_heartbeat WHERE component_key = ?", c.key());
        String lastError = queryString(
                "SELECT last_error FROM system_heartbeat WHERE component_key = ?", c.key());

        int interval = c.expectedIntervalSeconds();
        String status;
        Long ageSec = null;
        if (lastBeat == null) {
            // 한 번도 완주 못 함: 부팅 직후면 PENDING, interval x 5 넘겼으면 DOWN.
            status = uptimeSec < (long) interval * 5 ? "PENDING" : "DOWN";
        } else {
            ageSec = Duration.between(lastBeat, OffsetDateTime.now()).getSeconds();
            long baseline = Math.max(interval, observed == null ? 0 : observed);
            if (ageSec > baseline * 5) {
                status = "DOWN";
            } else if (ageSec > baseline * 3) {
                status = "DEGRADED";
            } else {
                status = "UP";
            }
        }
        return new CollectorStatus(c.key(), c.label(), c.metricSource(), status,
                lastBeat, ageSec, interval, observed, lastResult, lastError);
    }

    private long clockSkewSeconds() {
        try {
            Timestamp dbNow = jdbc.queryForObject("SELECT now()", Timestamp.class);
            return dbNow == null ? 0 : Math.abs(Duration.between(dbNow.toInstant(), Instant.now()).getSeconds());
        } catch (Exception ex) {
            return -1;   // 검사 불가
        }
    }

    private OffsetDateTime queryTimestamp(String sql, Object arg) {
        try {
            return jdbc.queryForObject(sql, OffsetDateTime.class, arg);
        } catch (Exception ex) {
            return null;
        }
    }

    private Integer queryInt(String sql, Object arg) {
        try {
            return jdbc.queryForObject(sql, Integer.class, arg);
        } catch (Exception ex) {
            return null;
        }
    }

    private String queryString(String sql, Object arg) {
        try {
            return jdbc.queryForObject(sql, String.class, arg);
        } catch (Exception ex) {
            return null;
        }
    }
}
