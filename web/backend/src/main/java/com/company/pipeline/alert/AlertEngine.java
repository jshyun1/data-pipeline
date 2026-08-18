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

    // 연쇄억제(원본 5-7): 뚜렷한 상위원인이 없어도 같은 유형 알림이 이 수 이상 대량 발생하면
    // 대표 1건만 남기고 나머지를 접는다.
    private static final int MASS_SUPPRESS_THRESHOLD = 3;

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
            // --- 원본 5-1 / PDF 3쪽 규칙표의 나머지 6종 ---
            seedRule("SERVER_DISK", "디스크 임계", "HOST", false, "WARNING", "INFO",
                    "disk.used.pct", "파일시스템 사용률", "{\"threshold\":80,\"clear\":72}", 120, 300);
            seedRule("CDC_LAG", "CDC 지연 임계", "DATA", false, "WARNING", "INFO",
                    "cdc.consumer.lag", "Kafka 미처리 건수", "{\"threshold\":50000,\"clear\":10000}", 300, 300);
            seedRule("CONNECTOR_FAILED", "커넥터 FAILED", "PROCESS", true, "CRITICAL", "WARNING",
                    "connector.state", "Kafka Connect 커넥터 실패", "{}", 0, 300);
            seedRule("SERVICE_UNREACHABLE", "서비스 응답 없음", "PROCESS", true, "CRITICAL", "WARNING",
                    "heartbeat.failure", "NiFi·Airflow 등 대상 서비스 무응답",
                    "{\"consecutive_failures\":3}", 0, 300);
            seedRule("JOB_CONSECUTIVE_FAILURE", "연속 실패", "JOB", false, "CRITICAL", "WARNING",
                    "job.run.status", "동일 잡 연속 실패", "{\"count\":3}", 0, 1800);
            seedRule("JOB_NOT_RUN", "장기 미실행", "JOB", false, "WARNING", "INFO",
                    "job.run.age", "일정 기간 실행 이력 없음", "{\"days\":7}", 0, 0);
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
        // 원본 5-1 규칙표 나머지. 하나가 죽어도 나머지는 계속 돈다.
        evalSafely("SERVER_DISK", this::diskSignals);
        evalSafely("CDC_LAG", this::cdcLagSignals);
        evalSafely("CONNECTOR_FAILED", this::connectorFailedSignals);
        evalSafely("SERVICE_UNREACHABLE", this::serviceUnreachableSignals);
        evalSafely("JOB_CONSECUTIVE_FAILURE", this::consecutiveFailureSignals);
        evalSafely("JOB_NOT_RUN", this::notRunSignals);
        // 모든 규칙이 인스턴스를 만든 뒤, 연쇄로 쏟아진 하위 알림을 상위원인 아래로 접는다.
        try {
            evaluateChainSuppression();
        } catch (Exception ex) {
            log.warn("연쇄억제 실패: {}", ex.getMessage());
        }
    }

    /**
     * 연쇄억제(원본 5-7). 상위원인 하나로 하위 알림이 대량 발생하는 상황(예: Kafka Broker 장애 →
     * 커넥터 전부 FAILED)에서 대기열이 폭주하지 않게, 하위 알림을 상위원인 아래로 접는다
     * (suppressed_by). 대기열/이력은 suppressed_by 로 걸러 대표만 노출하고 나머지는 "억제 N"으로 센다.
     */
    private void evaluateChainSuppression() {
        // 1) 억제 해제: 억제자(suppressor)가 닫혔거나 더는 FIRING 이 아니면 접기를 푼다.
        jdbc.update("""
                UPDATE alert_instance c SET suppressed_by = NULL, updated_at = now()
                WHERE c.suppressed_by IS NOT NULL AND c.closed_at IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM alert_instance p
                    WHERE p.id = c.suppressed_by AND p.closed_at IS NULL AND p.state = 'FIRING')
                """);

        // 2) 상위원인 있음: SERVICE_UNREACHABLE(브로커/서비스 응답없음)가 FIRING 이면
        //    열린 CONNECTOR_FAILED 를 그 아래로 접는다.
        Long root = jdbc.query("""
                SELECT id FROM alert_instance
                WHERE rule_type_code = 'SERVICE_UNREACHABLE' AND state = 'FIRING' AND closed_at IS NULL
                ORDER BY started_at LIMIT 1
                """, (rs, i) -> rs.getLong(1)).stream().findFirst().orElse(null);
        if (root != null) {
            int n = jdbc.update("""
                    UPDATE alert_instance SET suppressed_by = ?, updated_at = now()
                    WHERE rule_type_code = 'CONNECTOR_FAILED' AND closed_at IS NULL
                      AND suppressed_by IS NULL AND id <> ?
                    """, root, root);
            if (n > 0) {
                log.info("연쇄억제: SERVICE 상위원인({}) 아래 CONNECTOR {}건 접기", root, n);
            }
            return;
        }

        // 3) 상위원인 없음: 같은 유형(CONNECTOR_FAILED)이 임계 이상 대량이면
        //    가장 오래된 것을 대표로 두고 나머지를 접는다.
        List<Long> conns = jdbc.query("""
                SELECT id FROM alert_instance
                WHERE rule_type_code = 'CONNECTOR_FAILED' AND closed_at IS NULL AND suppressed_by IS NULL
                ORDER BY started_at
                """, (rs, i) -> rs.getLong(1));
        if (conns.size() >= MASS_SUPPRESS_THRESHOLD) {
            long rep = conns.get(0);
            int n = jdbc.update("""
                    UPDATE alert_instance SET suppressed_by = ?, updated_at = now()
                    WHERE rule_type_code = 'CONNECTOR_FAILED' AND closed_at IS NULL
                      AND suppressed_by IS NULL AND id <> ?
                    """, rep, rep);
            if (n > 0) {
                log.info("연쇄억제(대량): 대표({}) 아래 CONNECTOR {}건 접기", rep, n);
            }
        }
    }

    private void evalSafely(String ruleType,
                            java.util.function.Function<Map<String, Object>, List<Candidate>> signalFn) {
        try {
            evaluateWithCandidates(ruleType + ":all", ruleType, signalFn);
        } catch (Exception ex) {
            log.warn("알림 평가 실패({}): {}", ruleType, ex.getMessage());
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
        // 조건이 실제로 해제되면 자동으로 확인(ack) 처리한다 — 해소된 건이 «미확인»으로 남아
        // 종 배지를 부풀리지 않게. 사람이 이미 확인했으면 그 기록을 유지한다.
        jdbc.update("UPDATE alert_instance SET state='RESOLVED', resolved_at=now(), closed_at=now(), "
                + "display_until=now() + interval '24 hours', resolve_reason='CONDITION_CLEARED', "
                + "ack_by=COALESCE(ack_by, 'SYSTEM'), ack_at=COALESCE(ack_at, now()), "
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

    // ================================================================================
    // 원본 문서 5-1 / PDF 3쪽 판정 규칙표 나머지 항목 (U9 확장)
    //
    // 대기열 화면은 있는데 그 안을 채울 규칙이 4종뿐이었다. 규칙표에서 신호 원천이 이미
    // DB 에 저장되어 있는 6종을 추가한다. 설계 원칙 B("판정기는 외부 시스템을 절대 호출하지
    // 않는다")를 지키려고 전부 저장된 관측값만 읽는다 — NiFi/Airflow 를 여기서 부르지 않는다.
    // ================================================================================

    /** 한 규칙이 지금 발화해야 한다고 본 대상 하나. targetKey 가 인스턴스의 동일성 기준이다. */
    private record Candidate(String targetKey, String targetLabel, String componentCode,
                             Double observed, Double threshold, String summary, String deepLink) {}

    /** 규칙 유형과 내장 규칙을 한 번에 등록한다(이미 있으면 아무것도 하지 않는다). */
    private void seedRule(String code, String label, String category, boolean mandatory,
                          String defaultSeverity, String minSeverity, String signalKey,
                          String description, String paramsJson, int forSeconds, int clearSeconds) {
        jdbc.update("""
                INSERT INTO alert_rule_type
                    (code, label, category, kpi_axis, mandatory, default_severity, min_severity,
                     signal_keys_req, description)
                VALUES (?, ?, ?, 'CURRENT', ?, ?, ?, ?, ?)
                ON CONFLICT (code) DO NOTHING
                """, code, label, category, mandatory, defaultSeverity, minSeverity, signalKey, description);
        String builtinKey = code + ":all";
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                Integer.class, builtinKey);
        if (n != null && n == 0) {
            jdbc.update("""
                    INSERT INTO alert_rule
                        (rule_type_code, builtin_key, name, severity, params_json, for_seconds, clear_seconds, mandatory)
                    VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                    """, code, builtinKey, label, defaultSeverity, paramsJson, forSeconds, clearSeconds, mandatory);
            log.info("내장 규칙 시드: {}", builtinKey);
        }
    }

    /**
     * 공통 상태기계. 후보에 있으면 PENDING→FIRING 으로 밀어올리고, 후보에서 빠진 열린 인스턴스는
     * clear_seconds 를 채운 뒤 해소한다.
     *
     * <p>메모리 규칙의 60줄짜리 상태 전이를 규칙마다 복사하면 언젠가 한 곳만 고쳐져서
     * 규칙마다 다르게 동작하게 된다. 전이 규약은 한 군데만 둔다.
     */
    private void evaluateWithCandidates(String builtinKey, String ruleType,
                                        java.util.function.Function<Map<String, Object>, List<Candidate>> signalFn) {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, for_seconds, clear_seconds, params_json::text AS params, enabled "
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL", builtinKey);
        } catch (Exception ex) {
            return;   // 아직 시드 안 됨
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        long ruleId = ((Number) rule.get("id")).longValue();
        String severity = (String) rule.get("severity");
        int forSec = ((Number) rule.get("for_seconds")).intValue();
        int clearSec = ((Number) rule.get("clear_seconds")).intValue();

        List<Candidate> candidates = signalFn.apply(rule);
        if (candidates == null) {
            // 신호 자체를 못 읽은 경우. "모름"을 "정상"으로 칠하지 않으려면 해소도 하지 않는다.
            return;
        }
        Set<String> firingKeys = new HashSet<>();
        for (Candidate c : candidates) {
            firingKeys.add(c.targetKey());
            advance(ruleId, ruleType, severity, forSec, c);
        }
        // 후보에서 빠진 열린 인스턴스 = 조건 해제 진행 중
        for (Map<String, Object> open : jdbc.queryForList(
                "SELECT id, state, target_key, false_observed_sec FROM alert_instance "
                        + "WHERE rule_id=? AND closed_at IS NULL", ruleId)) {
            if (firingKeys.contains((String) open.get("target_key"))) {
                continue;
            }
            long falseSec = ((Number) open.get("false_observed_sec")).longValue() + EVAL_INTERVAL_SEC;
            if (falseSec >= clearSec) {
                resolve((Number) open.get("id"), (String) open.get("state"));
            } else {
                jdbc.update("UPDATE alert_instance SET false_observed_sec=?, true_observed_sec=0, "
                        + "last_evaluated_at=now(), updated_at=now() WHERE id=?", falseSec, open.get("id"));
            }
        }
    }

    private void advance(long ruleId, String ruleType, String severity, int forSec, Candidate c) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, state, true_observed_sec FROM alert_instance "
                        + "WHERE rule_id=? AND target_key=? AND closed_at IS NULL", ruleId, c.targetKey());
        if (rows.isEmpty()) {
            String state = forSec <= EVAL_INTERVAL_SEC ? "FIRING" : "PENDING";
            Long id = jdbc.queryForObject("""
                    INSERT INTO alert_instance
                        (rule_id, rule_type_code, kpi_axis, target_key, target_label, component_code, severity, state,
                         condition_since, true_observed_sec, started_at, last_evaluated_at, last_transition_at,
                         observed_value, threshold_value, summary, deep_link)
                    VALUES (?, ?, 'CURRENT', ?, ?, ?, ?, ?, now(), ?, CASE WHEN ?='FIRING' THEN now() END,
                            now(), now(), ?, ?, ?, ?)
                    RETURNING id
                    """, Long.class, ruleId, ruleType, c.targetKey(), c.targetLabel(), c.componentCode(),
                    severity, state, EVAL_INTERVAL_SEC, state, c.observed(), c.threshold(),
                    c.summary(), c.deepLink());
            event(id, "CREATED", null, state, "SYSTEM");
            if ("FIRING".equals(state)) {
                event(id, "FIRED", "PENDING", "FIRING", "SYSTEM");
                notifyFired(id);
            }
            return;
        }
        Map<String, Object> open = rows.get(0);
        long trueSec = ((Number) open.get("true_observed_sec")).longValue() + EVAL_INTERVAL_SEC;
        if ("PENDING".equals(open.get("state")) && trueSec >= forSec) {
            jdbc.update("UPDATE alert_instance SET state='FIRING', true_observed_sec=?, started_at=now(), "
                    + "observed_value=?, summary=?, last_transition_at=now(), last_evaluated_at=now(), "
                    + "updated_at=now(), version=version+1 WHERE id=?",
                    trueSec, c.observed(), c.summary(), open.get("id"));
            long id = ((Number) open.get("id")).longValue();
            event(id, "FIRED", "PENDING", "FIRING", "SYSTEM");
            notifyFired(id);
        } else {
            jdbc.update("UPDATE alert_instance SET true_observed_sec=?, false_observed_sec=0, observed_value=?, "
                    + "summary=?, last_evaluated_at=now(), updated_at=now() WHERE id=?",
                    trueSec, c.observed(), c.summary(), open.get("id"));
        }
    }

    /** 디스크 사용률 임계(원본 규칙표 "디스크 > 80%", 경고). 마운트별로 인스턴스를 따로 연다. */
    private List<Candidate> diskSignals(Map<String, Object> rule) {
        double threshold = jsonNum(rule.get("params").toString(), "threshold", 80);
        return jdbc.query("""
                SELECT DISTINCT ON (target_key) target_key, used_percent
                FROM infra_resource_sample
                WHERE metric='DISK' AND sampled_at > now() - interval '5 minutes' AND used_percent IS NOT NULL
                ORDER BY target_key, sampled_at DESC
                """, (rs, i) -> {
            double pct = rs.getDouble("used_percent");
            if (pct <= threshold) {
                return null;
            }
            String mount = rs.getString("target_key");
            String label = (mount == null || mount.isBlank()) ? "루트" : mount;
            return new Candidate("DISK:" + label, label, "HOST", pct, threshold,
                    String.format("디스크 %s 사용률 %.1f%% (임계 %.0f%%)", label, pct, threshold),
                    "/dashboard#infra");
        }).stream().filter(java.util.Objects::nonNull).toList();
    }

    /** CDC 미처리 임계(원본 규칙표 "CDC 지연 > 5만 건", 경고). 파이프라인별로 연다. */
    private List<Candidate> cdcLagSignals(Map<String, Object> rule) {
        double threshold = jsonNum(rule.get("params").toString(), "threshold", 50000);
        return jdbc.query("""
                SELECT DISTINCT ON (s.pipeline_id) s.pipeline_id, s.consumer_lag, d.name
                FROM pipeline_metric_snapshot s
                LEFT JOIN pipeline_definition d ON d.id = s.pipeline_id
                WHERE s.collected_at > now() - interval '10 minutes' AND s.consumer_lag IS NOT NULL
                ORDER BY s.pipeline_id, s.collected_at DESC
                """, (rs, i) -> {
            long lag = rs.getLong("consumer_lag");
            if (lag <= threshold) {
                return null;
            }
            long pid = rs.getLong("pipeline_id");
            String name = rs.getString("name");
            String label = name != null ? name : ("파이프라인 " + pid);
            return new Candidate("CDC_LAG:" + pid, label, "CDC", (double) lag, threshold,
                    String.format("%s — 미처리 %,d건 (임계 %,.0f건)", label, lag, threshold),
                    "/cdc/logs");
        }).stream().filter(java.util.Objects::nonNull).toList();
    }

    /** 커넥터 FAILED(원본 규칙표, 위험). 최신 스냅샷의 소스/싱크 상태를 본다. */
    private List<Candidate> connectorFailedSignals(Map<String, Object> rule) {
        return jdbc.query("""
                SELECT DISTINCT ON (s.pipeline_id) s.pipeline_id, d.name,
                       s.connector_state, s.source_connector_state, s.sink_connector_state
                FROM pipeline_metric_snapshot s
                LEFT JOIN pipeline_definition d ON d.id = s.pipeline_id
                WHERE s.collected_at > now() - interval '10 minutes'
                ORDER BY s.pipeline_id, s.collected_at DESC
                """, (rs, i) -> {
            String source = rs.getString("source_connector_state");
            String sink = rs.getString("sink_connector_state");
            String single = rs.getString("connector_state");
            String failed = "FAILED".equalsIgnoreCase(source) ? "Source"
                    : "FAILED".equalsIgnoreCase(sink) ? "Sink"
                    : "FAILED".equalsIgnoreCase(single) ? "커넥터" : null;
            if (failed == null) {
                return null;
            }
            long pid = rs.getLong("pipeline_id");
            String name = rs.getString("name");
            String label = name != null ? name : ("파이프라인 " + pid);
            return new Candidate("CONNECTOR:" + pid, label, "CDC", null, null,
                    String.format("%s — %s 커넥터 FAILED", label, failed), "/cdc/pipelines");
        }).stream().filter(java.util.Objects::nonNull).toList();
    }

    /**
     * NiFi·Airflow 응답 없음(원본 규칙표, 위험).
     *
     * <p>여기서 NiFi 를 직접 부르지 않는다(원칙 B). 그 서비스를 대상으로 도는 수집기가 실패를
     * 연속으로 기록하고 있다는 사실 자체가 "응답 없음"의 저장된 증거다.
     */
    private List<Candidate> serviceUnreachableSignals(Map<String, Object> rule) {
        double minFailures = jsonNum(rule.get("params").toString(), "consecutive_failures", 3);
        return jdbc.query("""
                SELECT metric_source, component_label, consecutive_failures, last_error
                FROM system_heartbeat
                WHERE consecutive_failures >= ? AND metric_source IS NOT NULL
                """, (rs, i) -> {
            String source = rs.getString("metric_source");
            int fails = rs.getInt("consecutive_failures");
            String err = rs.getString("last_error");
            return new Candidate("SERVICE:" + source, source, source, (double) fails, minFailures,
                    String.format("%s 응답 없음 — 수집 연속 실패 %d회%s", source, fails,
                            err == null || err.isBlank() ? "" : " (" + err + ")"),
                    "/self-check");
        }, (int) minFailures);
    }

    /** 연속 N회 실패(원본 규칙표, 위험). 잡별 최근 실행을 훑어 연속 실패 길이를 센다. */
    private List<Candidate> consecutiveFailureSignals(Map<String, Object> rule) {
        int need = (int) jsonNum(rule.get("params").toString(), "count", 3);
        return jdbc.query("""
                WITH ranked AS (
                    SELECT r.job_id, r.status, r.started_at,
                           row_number() OVER (PARTITION BY r.job_id ORDER BY r.started_at DESC) AS rn
                    FROM etl_job_run r
                    WHERE r.started_at > now() - interval '30 days'
                ),
                streak AS (
                    SELECT job_id, count(*) AS fails
                    FROM ranked
                    WHERE rn <= ? AND status = 'FAILED'
                    GROUP BY job_id
                )
                SELECT s.job_id, s.fails, j.job_name
                FROM streak s LEFT JOIN etl_job j ON j.id = s.job_id
                WHERE s.fails >= ?
                """, (rs, i) -> {
            long jobId = rs.getLong("job_id");
            int fails = rs.getInt("fails");
            String name = rs.getString("job_name");
            String label = name != null ? name : ("잡 " + jobId);
            return new Candidate("JOB_STREAK:" + jobId, label, "ETL", (double) fails, (double) need,
                    String.format("%s — 최근 %d회 연속 실패", label, fails), "/etl/logs?jobId=" + jobId);
        }, need, need);
    }

    /** N일 이상 미실행(원본 규칙표, 경고). 한 번은 돌았던 잡만 대상으로 한다. */
    private List<Candidate> notRunSignals(Map<String, Object> rule) {
        int days = (int) jsonNum(rule.get("params").toString(), "days", 7);
        return jdbc.query("""
                SELECT r.job_id, j.job_name, max(r.started_at) AS last_run,
                       EXTRACT(DAY FROM (now() - max(r.started_at)))::int AS idle_days
                FROM etl_job_run r
                LEFT JOIN etl_job j ON j.id = r.job_id
                WHERE j.deleted_at IS NULL
                GROUP BY r.job_id, j.job_name
                HAVING max(r.started_at) < now() - make_interval(days => ?)
                """, (rs, i) -> {
            long jobId = rs.getLong("job_id");
            int idle = rs.getInt("idle_days");
            String name = rs.getString("job_name");
            String label = name != null ? name : ("잡 " + jobId);
            return new Candidate("JOB_IDLE:" + jobId, label, "ETL", (double) idle, (double) days,
                    String.format("%s — %d일간 실행 없음", label, idle), "/airflow/dashboard");
        }, days);
    }

}
