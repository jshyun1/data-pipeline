package com.company.pipeline.alert;

import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private static final String JOB_RULE_TYPE = "JOB_FAILURE";
    private static final String JOB_BUILTIN_KEY = "JOB_FAILURE:all";

    private static final String DATA_RULE_TYPE = "DATA_FRESHNESS";
    private static final String DATA_BUILTIN_KEY = "DATA_FRESHNESS:all";

    private static final String COLLECTOR_RULE_TYPE = "COLLECTOR_DOWN";
    private static final String COLLECTOR_BUILTIN_KEY = "COLLECTOR_DOWN:all";

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
            // JOB 실패(필수 알림 3종 중 하나). 이벤트 기반이라 for_seconds=0.
            jdbc.update("""
                    INSERT INTO alert_rule_type
                        (code, label, category, kpi_axis, mandatory, default_severity, min_severity, signal_keys_req, description)
                    VALUES (?, 'ETL Job 실패', 'JOB', 'CURRENT', true, 'WARNING', 'WARNING', 'job.run.status', 'Job 실행 실패')
                    ON CONFLICT (code) DO NOTHING
                    """, JOB_RULE_TYPE);
            Integer jn = jdbc.queryForObject(
                    "SELECT count(*) FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    Integer.class, JOB_BUILTIN_KEY);
            if (jn != null && jn == 0) {
                jdbc.update("""
                        INSERT INTO alert_rule
                            (rule_type_code, builtin_key, name, severity, params_json, for_seconds, clear_seconds, mandatory)
                        VALUES (?, ?, 'ETL Job 실패', 'WARNING', '{}'::jsonb, 0, 1800, true)
                        """, JOB_RULE_TYPE, JOB_BUILTIN_KEY);
                log.info("내장 규칙 시드: {}", JOB_BUILTIN_KEY);
            }
            // DATA 신선도(적재 정체). 24h 내 활동한 소스가 임계(분)만큼 새 입력이 없으면 발화.
            jdbc.update("""
                    INSERT INTO alert_rule_type
                        (code, label, category, kpi_axis, mandatory, default_severity, min_severity, signal_keys_req, description)
                    VALUES (?, '적재 정체(신선도)', 'DATA', 'CURRENT', false, 'WARNING', 'INFO', 'load.rollup.age', '파이프라인 적재 정체')
                    ON CONFLICT (code) DO NOTHING
                    """, DATA_RULE_TYPE);
            Integer dn = jdbc.queryForObject(
                    "SELECT count(*) FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    Integer.class, DATA_BUILTIN_KEY);
            if (dn != null && dn == 0) {
                jdbc.update("""
                        INSERT INTO alert_rule
                            (rule_type_code, builtin_key, name, severity, params_json, for_seconds, clear_seconds, mandatory)
                        VALUES (?, ?, '적재 정체(신선도)', 'WARNING', ?::jsonb, 0, 0, false)
                        """, DATA_RULE_TYPE, DATA_BUILTIN_KEY, "{\"staleness_minutes\":30}");
                log.info("내장 규칙 시드: {}", DATA_BUILTIN_KEY);
            }
            // 수집기 중단(데드맨). WatchdogService 가 연 collector_outage 를 대기열 알림으로 승격.
            jdbc.update("""
                    INSERT INTO alert_rule_type
                        (code, label, category, kpi_axis, mandatory, default_severity, min_severity, signal_keys_req, description)
                    VALUES (?, '수집기 중단', 'HOST', 'CURRENT', true, 'CRITICAL', 'WARNING', 'heartbeat.outage', '신호 수집기 결측')
                    ON CONFLICT (code) DO NOTHING
                    """, COLLECTOR_RULE_TYPE);
            Integer cn = jdbc.queryForObject(
                    "SELECT count(*) FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    Integer.class, COLLECTOR_BUILTIN_KEY);
            if (cn != null && cn == 0) {
                jdbc.update("""
                        INSERT INTO alert_rule
                            (rule_type_code, builtin_key, name, severity, params_json, for_seconds, clear_seconds, mandatory)
                        VALUES (?, ?, '수집기 중단', 'CRITICAL', '{}'::jsonb, 0, 0, true)
                        """, COLLECTOR_RULE_TYPE, COLLECTOR_BUILTIN_KEY);
                log.info("내장 규칙 시드: {}", COLLECTOR_BUILTIN_KEY);
            }
        } catch (Exception ex) {
            log.warn("내장 규칙 시드 실패(기동은 계속): {}", ex.getMessage());
        }
    }

    @Scheduled(fixedRate = 20_000, initialDelay = 45_000, scheduler = "controlPlaneScheduler")
    public void evaluate() {
        // 한 규칙의 실패가 평가 루프를 죽이면 안 된다(다른 규칙/다음 주기 계속).
        try {
            evaluateMemoryRule();
        } catch (Exception ex) {
            log.warn("알림 평가 실패({}): {}", BUILTIN_KEY, ex.getMessage());
        }
        try {
            evaluateJobRules();
        } catch (Exception ex) {
            log.warn("알림 평가 실패({}): {}", JOB_BUILTIN_KEY, ex.getMessage());
        }
        try {
            evaluateDataFreshness();
        } catch (Exception ex) {
            log.warn("알림 평가 실패({}): {}", DATA_BUILTIN_KEY, ex.getMessage());
        }
        try {
            evaluateCollectorOutage();
        } catch (Exception ex) {
            log.warn("알림 평가 실패({}): {}", COLLECTOR_BUILTIN_KEY, ex.getMessage());
        }
    }

    /** JOB 실패(이벤트 기반). 최근 실패한 잡 실행마다 발화하고, 30분 새 실패가 없으면 해소한다. */
    private void evaluateJobRules() {
        Long ruleId = jdbc.query(
                "SELECT id FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL AND enabled",
                (rs, i) -> rs.getLong(1), JOB_BUILTIN_KEY).stream().findFirst().orElse(null);
        if (ruleId == null) {
            return;
        }
        // ended_at 은 timestamp without time zone. 앱 세션 TZ 로 now() 와 비교되므로,
        // 신호 기록(등록기)과 이 평가기는 반드시 같은 세션 TZ(앱 JVM 기본)에서 돈다.
        List<Map<String, Object>> failed = jdbc.queryForList("""
                SELECT r.id AS run_id, r.job_id, j.job_name
                FROM etl_job_run r JOIN etl_job j ON j.id = r.job_id
                WHERE r.status = 'FAILED' AND r.ended_at > now() - interval '10 minutes'
                ORDER BY r.ended_at DESC
                """);
        for (Map<String, Object> f : failed) {
            long runId = ((Number) f.get("run_id")).longValue();
            long jobId = ((Number) f.get("job_id")).longValue();
            String jobName = (String) f.get("job_name");
            String target = "JOB:" + jobId;
            List<Map<String, Object>> open = jdbc.queryForList(
                    "SELECT id, last_failed_run_id FROM alert_instance WHERE rule_id=? AND target_key=? AND closed_at IS NULL",
                    ruleId, target);
            if (open.isEmpty()) {
                Long id = jdbc.queryForObject("""
                        INSERT INTO alert_instance
                            (rule_id, rule_type_code, kpi_axis, target_key, target_label, component_code, severity,
                             state, condition_since, started_at, last_evaluated_at, last_transition_at, summary,
                             last_failed_run_id, fail_run_count, deep_link)
                        VALUES (?, ?, 'CURRENT', ?, ?, 'JOB', 'WARNING', 'FIRING', now(), now(), now(), now(), ?, ?, 1,
                                '/dashboard?panel=jobs')
                        RETURNING id
                        """, Long.class, ruleId, JOB_RULE_TYPE, target, jobName,
                        "ETL Job 실패: " + jobName, runId);
                event(id, "CREATED", null, "FIRING", "SYSTEM");
                event(id, "FIRED", null, "FIRING", "SYSTEM");
                notifyFired(id);
                log.info("JOB 실패 알림 생성 {} job={}", id, jobName);
            } else {
                Map<String, Object> inst = open.get(0);
                Long lastRun = inst.get("last_failed_run_id") == null ? null
                        : ((Number) inst.get("last_failed_run_id")).longValue();
                if (lastRun == null || lastRun != runId) {
                    // 새 run 이 실패 - 반드시 다시 알린다.
                    Number id = (Number) inst.get("id");
                    jdbc.update("UPDATE alert_instance SET last_failed_run_id=?, fail_run_count=fail_run_count+1, "
                            + "last_evaluated_at=now(), last_transition_at=now(), updated_at=now() WHERE id=?", runId, id);
                    event(id.longValue(), "RECURRED", null, null, "SYSTEM");
                    notifyFired(id.longValue());
                }
            }
        }
        // 30분간 새 실패가 없으면(마지막 전이 후 clear window) 해소.
        jdbc.query("""
                SELECT id FROM alert_instance WHERE rule_id=? AND closed_at IS NULL
                  AND last_transition_at < now() - interval '30 minutes'
                """, (rs, i) -> rs.getLong(1), ruleId).forEach(id -> resolve(id, "FIRING"));
    }

    /** DATA 신선도(적재 정체). 24h 내 활동한 소스가 임계(분)만큼 새 입력이 없으면 발화, 재개되면 해소한다. */
    private void evaluateDataFreshness() {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, params_json::text AS params, enabled "
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL", DATA_BUILTIN_KEY);
        } catch (Exception ex) {
            return;   // 규칙 미시드
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        long ruleId = ((Number) rule.get("id")).longValue();
        String severity = (String) rule.get("severity");
        int stalenessMin = (int) jsonNum(rule.get("params").toString(), "staleness_minutes", 30);

        // 정체 소스: 최근 24h HOUR 롤업 활동이 있었으나, 최신 관측이 임계(분)보다 오래됨.
        // last_observed_at 은 timestamptz 라 세션 TZ 무관(신호기록·평가기 일관).
        List<Map<String, Object>> stalled = jdbc.queryForList("""
                SELECT pipeline_source,
                       EXTRACT(EPOCH FROM (now() - MAX(last_observed_at)))::bigint AS age_sec
                FROM pipeline_load_rollup
                WHERE granularity='HOUR' AND bucket_start >= now() - interval '24 hours'
                GROUP BY pipeline_source
                HAVING MAX(last_observed_at) < now() - (? * interval '1 minute')
                """, stalenessMin);
        Set<String> stalledSources = new HashSet<>();
        for (Map<String, Object> s : stalled) {
            String source = (String) s.get("pipeline_source");
            stalledSources.add(source);
            long ageSec = ((Number) s.get("age_sec")).longValue();
            String target = "DATA:" + source;
            List<Long> open = jdbc.query(
                    "SELECT id FROM alert_instance WHERE rule_id=? AND target_key=? AND closed_at IS NULL",
                    (rs, i) -> rs.getLong(1), ruleId, target);
            if (open.isEmpty()) {
                long ageMin = ageSec / 60;
                Long id = jdbc.queryForObject("""
                        INSERT INTO alert_instance
                            (rule_id, rule_type_code, kpi_axis, target_key, target_label, component_code, severity,
                             state, condition_since, started_at, last_evaluated_at, last_transition_at, summary,
                             observed_value, threshold_value, deep_link)
                        VALUES (?, ?, 'CURRENT', ?, ?, 'DATA', ?, 'FIRING', now(), now(), now(), now(), ?, ?, ?,
                                '/dashboard?panel=kpi')
                        RETURNING id
                        """, Long.class, ruleId, DATA_RULE_TYPE, target, source, severity,
                        "적재 정체: " + source + " (" + ageMin + "분 무입력)", (double) ageSec, (double) stalenessMin * 60);
                event(id, "CREATED", null, "FIRING", "SYSTEM");
                event(id, "FIRED", null, "FIRING", "SYSTEM");
                notifyFired(id);
                log.info("DATA 신선도 알림 생성 {} source={} age={}m", id, source, ageMin);
            } else {
                jdbc.update("UPDATE alert_instance SET observed_value=?, last_evaluated_at=now() WHERE id=?",
                        (double) ageSec, open.get(0));
            }
        }
        // 재개된 소스(더 이상 정체 아님)의 열린 인스턴스는 해소한다.
        List<Map<String, Object>> openData = jdbc.queryForList(
                "SELECT id, target_key FROM alert_instance WHERE rule_id=? AND closed_at IS NULL", ruleId);
        for (Map<String, Object> inst : openData) {
            String tk = (String) inst.get("target_key");
            String source = tk.startsWith("DATA:") ? tk.substring(5) : tk;
            if (!stalledSources.contains(source)) {
                resolve((Number) inst.get("id"), "FIRING");
            }
        }
    }

    /** 수집기 중단(데드맨). WatchdogService 가 연 collector_outage(미종료)를 대기열 알림으로 승격/해소한다. */
    private void evaluateCollectorOutage() {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, enabled FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    COLLECTOR_BUILTIN_KEY);
        } catch (Exception ex) {
            return;
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        long ruleId = ((Number) rule.get("id")).longValue();
        String severity = (String) rule.get("severity");

        List<Map<String, Object>> outages = jdbc.queryForList("""
                SELECT co.component_key,
                       COALESCE(sh.component_label, co.component_key) AS label,
                       EXTRACT(EPOCH FROM (now() - co.started_at))::bigint AS age_sec
                FROM collector_outage co
                LEFT JOIN system_heartbeat sh ON sh.component_key = co.component_key
                WHERE co.ended_at IS NULL
                """);
        Set<String> downKeys = new HashSet<>();
        for (Map<String, Object> o : outages) {
            String key = (String) o.get("component_key");
            downKeys.add(key);
            long ageSec = ((Number) o.get("age_sec")).longValue();
            String label = (String) o.get("label");
            String target = "COLLECTOR:" + key;
            List<Long> open = jdbc.query(
                    "SELECT id FROM alert_instance WHERE rule_id=? AND target_key=? AND closed_at IS NULL",
                    (rs, i) -> rs.getLong(1), ruleId, target);
            if (open.isEmpty()) {
                long ageMin = ageSec / 60;
                Long id = jdbc.queryForObject("""
                        INSERT INTO alert_instance
                            (rule_id, rule_type_code, kpi_axis, target_key, target_label, component_code, severity,
                             state, condition_since, started_at, last_evaluated_at, last_transition_at, summary,
                             observed_value, deep_link)
                        VALUES (?, ?, 'CURRENT', ?, ?, 'HOST', ?, 'FIRING', now(), now(), now(), now(), ?, ?,
                                '/self-check')
                        RETURNING id
                        """, Long.class, ruleId, COLLECTOR_RULE_TYPE, target, label, severity,
                        "수집기 중단: " + label + " (" + ageMin + "분 결측)", (double) ageSec);
                event(id, "CREATED", null, "FIRING", "SYSTEM");
                event(id, "FIRED", null, "FIRING", "SYSTEM");
                notifyFired(id);
                log.info("수집기 중단 알림 생성 {} key={} age={}m", id, key, ageMin);
            } else {
                jdbc.update("UPDATE alert_instance SET observed_value=?, last_evaluated_at=now() WHERE id=?",
                        (double) ageSec, open.get(0));
            }
        }
        // 복구된 수집기(outage 종료)의 열린 인스턴스는 해소한다.
        List<Map<String, Object>> openColl = jdbc.queryForList(
                "SELECT id, target_key FROM alert_instance WHERE rule_id=? AND closed_at IS NULL", ruleId);
        for (Map<String, Object> inst : openColl) {
            String tk = (String) inst.get("target_key");
            String key = tk.startsWith("COLLECTOR:") ? tk.substring(10) : tk;
            if (!downKeys.contains(key)) {
                resolve((Number) inst.get("id"), "FIRING");
            }
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
