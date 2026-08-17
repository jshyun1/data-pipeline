package com.company.pipeline.alert;

import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 알림 판정 엔진 — 수직 슬라이스 (U8/U9, 설계서 6-1절).
 *
 * <p>이 설계 최대 단위의 <b>동작하는 최소 골격</b>이다. 신호(infra_resource_sample) → 규칙 평가 →
 * 상태기계(PENDING→FIRING→RESOLVED) → alert_instance/event 까지 end-to-end 로 돈다. 우선 필수 규칙
 * 1종(SERVER_MEMORY, HOST:host)만 배선하고, 규칙 카탈로그 확장·백프레셔·연쇄억제·플래핑·발송 연동은
 * 후속 단위에서 얹는다.
 *
 * <p>원칙 준수: 평가기는 외부 시스템을 호출하지 않는다(신호 테이블만 읽는다). controlPlaneScheduler
 * 에서 돌아 수집(collect-)과 격리된다. for/clear 판정은 벽시계가 아니라 관측 누적 초(true/false
 * _observed_sec)로 한다.
 */
@Service
public class AlertEngine {

    private static final Logger log = LoggerFactory.getLogger(AlertEngine.class);

    private static final String RULE_TYPE = "SERVER_MEMORY";
    private static final String BUILTIN_KEY = "SERVER_MEMORY:host";
    private static final String TARGET_KEY = "HOST:host";
    private static final int EVAL_INTERVAL_SEC = 20;

    private final JdbcTemplate jdbc;
    private final SettingService settings;
    private final com.company.pipeline.notification.NotificationService notificationService;

    public AlertEngine(DataSource dataSource, SettingService settings,
                       com.company.pipeline.notification.NotificationService notificationService) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.settings = settings;
        this.notificationService = notificationService;
    }

    /** FIRING 확정 시 아웃박스 발송 + 사건 기록. */
    private void notifyFired(long id) {
        jdbc.update("UPDATE alert_instance SET notify_count=notify_count+1, last_notified_at=now() WHERE id=?", id);
        notificationService.enqueueForInstance(id);
        event(id, "NOTIFIED", null, null, "SYSTEM");
    }

    /** 내장 규칙 시드(코드가 원천). 부팅 시 없으면 만든다. */
    @EventListener(ApplicationReadyEvent.class)
    public void seedBuiltInRules() {
        try {
            jdbc.update("""
                    INSERT INTO alert_rule_type
                        (code, label, category, kpi_axis, mandatory, default_severity, min_severity, signal_keys_req, description)
                    VALUES (?, '서버 메모리 압박', 'HOST', 'CURRENT', true, 'CRITICAL', 'WARNING', 'mem.used.pct', '호스트 메모리 사용률')
                    ON CONFLICT (code) DO NOTHING
                    """, RULE_TYPE);
            Integer n = jdbc.queryForObject(
                    "SELECT count(*) FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    Integer.class, BUILTIN_KEY);
            if (n != null && n == 0) {
                jdbc.update("""
                        INSERT INTO alert_rule
                            (rule_type_code, builtin_key, name, severity, params_json, for_seconds, clear_seconds, mandatory)
                        VALUES (?, ?, '서버 메모리 압박', 'CRITICAL', ?::jsonb, 120, 300, true)
                        """, RULE_TYPE, BUILTIN_KEY, "{\"threshold\":80,\"clear\":72}");
                log.info("내장 규칙 시드: {}", BUILTIN_KEY);
            }
        } catch (Exception ex) {
            log.warn("내장 규칙 시드 실패(기동은 계속): {}", ex.getMessage());
        }
    }

    @Scheduled(fixedRate = 20_000, initialDelay = 45_000, scheduler = "controlPlaneScheduler")
    public void evaluate() {
        try {
            evaluateMemoryRule();
        } catch (Exception ex) {
            // 한 규칙의 실패가 평가 루프를 죽이면 안 된다(다른 규칙/다음 주기 계속).
            log.warn("알림 평가 실패({}): {}", BUILTIN_KEY, ex.getMessage());
        }
    }

    private void evaluateMemoryRule() {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, for_seconds, clear_seconds, params_json::text AS params, enabled "
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL", BUILTIN_KEY);
        } catch (Exception ex) {
            return;   // 규칙이 아직 시드 안 됨
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        long ruleId = ((Number) rule.get("id")).longValue();
        int forSec = ((Number) rule.get("for_seconds")).intValue();
        int clearSec = ((Number) rule.get("clear_seconds")).intValue();
        // 임계는 params 우선, 없으면 SettingKey. 여기선 params 고정 파싱(단순 슬라이스).
        double threshold = jsonNum(rule.get("params").toString(), "threshold", 80);
        double clearThreshold = jsonNum(rule.get("params").toString(), "clear", 72);

        // 신호: 최근 5분 내 최신 HOST 메모리 사용률.
        Double memPct = latestMemoryPct();
        if (memPct == null) {
            return;   // 신호 없음(미관측) - "모름"을 "정상"으로 칠하지 않는다. UNKNOWN 처리는 후속.
        }

        Map<String, Object> open = openInstance(ruleId);
        boolean over = memPct > threshold;
        boolean cleared = memPct < clearThreshold;

        if (over) {
            if (open == null) {
                createPending(ruleId, (String) rule.get("severity"), memPct, threshold, forSec);
            } else {
                String state = (String) open.get("state");
                long trueSec = ((Number) open.get("true_observed_sec")).longValue() + EVAL_INTERVAL_SEC;
                if ("PENDING".equals(state) && trueSec >= forSec) {
                    transitionToFiring((Number) open.get("id"), memPct, trueSec);
                } else {
                    jdbc.update("UPDATE alert_instance SET true_observed_sec=?, false_observed_sec=0, "
                            + "observed_value=?, last_evaluated_at=now(), updated_at=now() WHERE id=?",
                            trueSec, memPct, open.get("id"));
                }
            }
        } else if (cleared && open != null) {
            long falseSec = ((Number) open.get("false_observed_sec")).longValue() + EVAL_INTERVAL_SEC;
            if (falseSec >= clearSec) {
                resolve((Number) open.get("id"), (String) open.get("state"));
            } else {
                jdbc.update("UPDATE alert_instance SET false_observed_sec=?, true_observed_sec=0, "
                        + "observed_value=?, last_evaluated_at=now(), updated_at=now() WHERE id=?",
                        falseSec, memPct, open.get("id"));
            }
        } else if (open != null) {
            // 임계와 해제 사이 - 상태 유지, 관측값만 갱신.
            jdbc.update("UPDATE alert_instance SET observed_value=?, last_evaluated_at=now() WHERE id=?",
                    memPct, open.get("id"));
        }
    }

    private Double latestMemoryPct() {
        List<Double> vals = jdbc.query(
                "SELECT used_percent FROM infra_resource_sample "
                        + "WHERE scope='HOST' AND target_key='host' AND metric='MEMORY' "
                        + "AND sampled_at > now() - interval '5 minutes' AND used_percent IS NOT NULL "
                        + "ORDER BY sampled_at DESC LIMIT 1",
                (rs, i) -> rs.getDouble("used_percent"));
        return vals.isEmpty() ? null : vals.get(0);
    }

    private Map<String, Object> openInstance(long ruleId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, state, true_observed_sec, false_observed_sec FROM alert_instance "
                        + "WHERE rule_id=? AND target_key=? AND closed_at IS NULL", ruleId, TARGET_KEY);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void createPending(long ruleId, String severity, double memPct, double threshold, int forSec) {
        // for_seconds 가 평가주기 이하면 즉시 FIRING.
        String state = forSec <= EVAL_INTERVAL_SEC ? "FIRING" : "PENDING";
        Long id = jdbc.queryForObject("""
                INSERT INTO alert_instance
                    (rule_id, rule_type_code, kpi_axis, target_key, target_label, component_code, severity, state,
                     condition_since, true_observed_sec, started_at, last_evaluated_at, last_transition_at,
                     observed_value, threshold_value, summary, deep_link)
                VALUES (?, ?, 'CURRENT', ?, '운영 서버', 'HOST', ?, ?, now(), ?, CASE WHEN ?='FIRING' THEN now() END,
                        now(), now(), ?, ?, ?, '/dashboard?panel=resources&metric=memory')
                RETURNING id
                """, Long.class, ruleId, RULE_TYPE, TARGET_KEY, severity, state, EVAL_INTERVAL_SEC, state,
                memPct, threshold, summary(memPct, threshold));
        event(id, "CREATED", null, state, "SYSTEM");
        if ("FIRING".equals(state)) {
            event(id, "FIRED", "PENDING", "FIRING", "SYSTEM");
            notifyFired(id);
        }
        log.info("알림 인스턴스 생성 {} state={} mem={}%", id, state, memPct);
    }

    private void transitionToFiring(Number id, double memPct, long trueSec) {
        jdbc.update("UPDATE alert_instance SET state='FIRING', true_observed_sec=?, started_at=now(), "
                + "observed_value=?, last_transition_at=now(), last_evaluated_at=now(), updated_at=now(), "
                + "version=version+1 WHERE id=?", trueSec, memPct, id);
        event(((Number) id).longValue(), "FIRED", "PENDING", "FIRING", "SYSTEM");
        notifyFired(((Number) id).longValue());
        log.info("알림 FIRING 전이 {} mem={}%", id, memPct);
    }

    private void resolve(Number id, String fromState) {
        jdbc.update("UPDATE alert_instance SET state='RESOLVED', resolved_at=now(), closed_at=now(), "
                + "display_until=now() + interval '24 hours', resolve_reason='CONDITION_CLEARED', "
                + "last_transition_at=now(), last_evaluated_at=now(), updated_at=now(), version=version+1 WHERE id=?", id);
        event(((Number) id).longValue(), "RESOLVED", fromState, "RESOLVED", "SYSTEM");
        log.info("알림 해소 {} (조건 해제)", id);
    }

    private void event(Long instanceId, String type, String from, String to, String actor) {
        jdbc.update("INSERT INTO alert_instance_event (instance_id, event_type, from_state, to_state, actor, occurred_at) "
                + "VALUES (?, ?, ?, ?, ?, now())", instanceId, type, from, to, actor);
    }

    private String summary(double memPct, double threshold) {
        return String.format("메모리 %.1f%% (임계 %.0f%%)", memPct, threshold);
    }

    /** 아주 단순한 JSON 숫자 추출(params_json 슬라이스용). 정식 파서는 후속. */
    private double jsonNum(String json, String key, double def) {
        try {
            int i = json.indexOf("\"" + key + "\"");
            if (i < 0) {
                return def;
            }
            int c = json.indexOf(':', i) + 1;
            int e = c;
            while (e < json.length() && "0123456789.-".indexOf(json.charAt(e)) < 0) {
                e++;
            }
            int s = e;
            while (e < json.length() && "0123456789.-".indexOf(json.charAt(e)) >= 0) {
                e++;
            }
            return Double.parseDouble(json.substring(s, e).trim());
        } catch (Exception ex) {
            return def;
        }
    }
}
