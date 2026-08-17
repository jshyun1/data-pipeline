package com.company.pipeline.heartbeat;

import com.company.pipeline.heartbeat.HeartbeatComponentRegistry.Component;
import java.time.OffsetDateTime;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * 수집기 생존 상태를 DB(system_heartbeat)에 기록한다 (U5, 설계서 4-3/6-5).
 *
 * <p>기존 유일한 생존 근거였던 NifiPipelineMetricScheduler.lastCounterCheckAt 은 JVM 메모리
 * 필드라 재기동되면 "언제까지 살아 있었나"가 증발했다. 여기서는 주기 완주 시 DB 행을 갱신해
 * 재기동을 넘어 이력이 유지되게 한다. 판정 근거는 "행을 썼는가"가 아니라 "주기를 완주했는가"다 -
 * 각 수집기는 성공/대상0건 무관하게 beat 하고, 외부 조회 실패로 주기를 못 끝낸 경우엔 beat 하지 않는다.
 */
@Service
public class HeartbeatService {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    private final JdbcTemplate jdbc;

    public HeartbeatService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    /**
     * 기동 시: 기대 컴포넌트 행을 (last_beat_at NULL 로) 보장하고, 재기동으로 생긴 결측 구간을
     * STARTUP 으로 기록한다. last_beat_at 을 now() 로 시드하지 않는다(UTC/KST 차로 즉시 오탐).
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        for (Component c : HeartbeatComponentRegistry.COMPONENTS) {
            try {
                jdbc.update("""
                        INSERT INTO system_heartbeat
                            (component_key, component_label, metric_source, expected_interval_seconds)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (component_key) DO UPDATE
                           SET component_label = EXCLUDED.component_label,
                               metric_source = EXCLUDED.metric_source,
                               expected_interval_seconds = EXCLUDED.expected_interval_seconds
                        """, c.key(), c.label(), c.metricSource(), c.expectedIntervalSeconds());
                openStartupOutageIfGap(c);
            } catch (Exception ex) {
                log.warn("하트비트 컴포넌트 {} 초기화 실패(기동은 계속): {}", c.key(), ex.getMessage());
            }
        }
    }

    /** 재기동 전 마지막 beat 가 expected x 3 보다 오래됐으면 그 구간을 STARTUP 결측으로 연다. */
    private void openStartupOutageIfGap(Component c) {
        OffsetDateTime lastBeat = lastBeat(c.key());
        if (lastBeat == null) {
            return;   // 한 번도 안 뜬 신규 설치 - 결측 아님
        }
        long ageSec = java.time.Duration.between(lastBeat, OffsetDateTime.now()).getSeconds();
        if (ageSec <= (long) c.expectedIntervalSeconds() * 3) {
            return;   // 정상 범위(배포 재기동이 빨랐음)
        }
        // 열린 구간이 없을 때만 연다(부분 UNIQUE 제약이 중복도 막는다).
        Integer open = jdbc.queryForObject(
                "SELECT count(*) FROM collector_outage WHERE component_key = ? AND ended_at IS NULL",
                Integer.class, c.key());
        if (open != null && open == 0) {
            jdbc.update("""
                    INSERT INTO collector_outage (component_key, started_at, detected_by, reason)
                    VALUES (?, ?, 'STARTUP', ?)
                    """, c.key(), lastBeat, "앱 재기동으로 " + ageSec + "초 관측 공백");
            log.info("하트비트 {} 재기동 결측 기록(STARTUP, {}초 공백)", c.key(), ageSec);
        }
    }

    /** 주기 정상 완주. 행을 갱신하고 관측 간격(직전 beat 와의 차)을 기록한다. */
    public void beat(String componentKey) {
        try {
            jdbc.update("""
                    UPDATE system_heartbeat SET
                        observed_interval_seconds = CASE WHEN last_beat_at IS NOT NULL
                            THEN GREATEST(1, EXTRACT(EPOCH FROM (now() - last_beat_at))::int)
                            ELSE observed_interval_seconds END,
                        first_observed_at = COALESCE(first_observed_at, now()),
                        last_beat_at = now(),
                        last_result = 'OK',
                        last_error = NULL,
                        consecutive_failures = 0,
                        updated_at = now()
                    WHERE component_key = ?
                    """, componentKey);
        } catch (Exception ex) {
            // 하트비트 기록 실패가 수집 자체를 막으면 안 된다.
            log.warn("하트비트 {} 기록 실패: {}", componentKey, ex.getMessage());
        }
    }

    /** 주기가 실패로 끝남(외부 조회 예외 등). 연속 실패 카운트를 올린다. */
    public void beatFailed(String componentKey, String error) {
        try {
            jdbc.update("""
                    UPDATE system_heartbeat SET
                        last_result = 'FAILED',
                        last_error = ?,
                        consecutive_failures = consecutive_failures + 1,
                        updated_at = now()
                    WHERE component_key = ?
                    """, error, componentKey);
        } catch (Exception ex) {
            log.warn("하트비트 {} 실패기록 실패: {}", componentKey, ex.getMessage());
        }
    }

    public OffsetDateTime lastBeat(String componentKey) {
        try {
            return jdbc.queryForObject(
                    "SELECT last_beat_at FROM system_heartbeat WHERE component_key = ?",
                    OffsetDateTime.class, componentKey);
        } catch (Exception ex) {
            return null;
        }
    }
}
