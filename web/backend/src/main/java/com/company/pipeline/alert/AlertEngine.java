package com.company.pipeline.alert;

import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient;
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
    /** 워크플로우 규칙의 신호원. 우리 DB 사본이 아니라 Airflow 를 직접 본다. */
    private final com.company.pipeline.airflowdashboard.AirflowDagRunClient dagRunClient;
    private static final long DAGRUN_CACHE_MS = 10_000L;
    private volatile List<AirflowDagRunClient.DagRunWithDag> dagRunCache;
    private volatile long dagRunCachedAt;

    public AlertEngine(DataSource dataSource, SettingService settings,
                       com.company.pipeline.notification.NotificationService notificationService,
                       com.company.pipeline.airflowdashboard.AirflowDagRunClient dagRunClient) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.settings = settings;
        this.notificationService = notificationService;
        this.dagRunClient = dagRunClient;
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
            seedRule("WORKFLOW_FAILURE", "워크플로우 실패", "JOB", false, "CRITICAL", "WARNING",
                    "airflow.dagrun.state", "워크플로우 DAG 실행이 실패로 끝남", "{}", 0, 300);
            seedRule("WORKFLOW_NOT_COMPLETED", "워크플로우 미완료", "JOB", false, "WARNING", "INFO",
                    "airflow.dagrun.state", "정기 실행인데 성공한 실행이 없음",
                    "{\"grace_minutes\":1440}", 0, 300);
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
        evalSafely("JOB_NOT_RUN", this::notRunSignals);
        evalSafely("WORKFLOW_FAILURE", this::workflowFailureSignals);
        evalSafely("WORKFLOW_NOT_COMPLETED", this::workflowNotCompletedSignals);
        // 모든 규칙이 인스턴스를 만든 뒤, 연쇄로 쏟아진 하위 알림을 상위원인 아래로 접는다.
        try {
            evaluateChainSuppression();
        } catch (Exception ex) {
            log.warn("연쇄억제 실패: {}", ex.getMessage());
        }
        try {
            evaluateRenotify();
        } catch (Exception ex) {
            log.warn("재발송 실패: {}", ex.getMessage());
        }
    }

    /**
     * 재발송 정책(규칙별). 진행 중·미확인·미종료 알림이 규칙의 renotify_seconds 간격을 지났고 아직
     * 최대 발송 횟수(params.notify_max) 미만이면 다시 알린다. 예: 즉시 1회 + 5분마다 + 총 5회.
     * (확인하거나 해소되면 자동으로 멈춘다 — ack_at·closed_at 조건.)
     */
    private void evaluateRenotify() {
        List<Map<String, Object>> due = jdbc.queryForList("""
                SELECT ai.id, ai.notify_count, r.params_json::text AS params
                FROM alert_instance ai JOIN alert_rule r ON r.id = ai.rule_id
                WHERE ai.closed_at IS NULL AND ai.state = 'FIRING'
                  AND ai.ack_at IS NULL AND ai.suppressed_by IS NULL
                  -- 끄거나 지운 규칙은 새 판정을 안 하는데(각 평가기가 enabled 를 본다),
                  -- 여기만 안 보면 «끄기 전에 열려 있던» 알림이 최대 횟수까지 계속 나간다.
                  AND r.enabled AND r.deleted_at IS NULL
                  AND r.renotify_seconds > 0
                  AND NOT r.schedule_enabled
                  AND ai.last_notified_at IS NOT NULL
                  AND ai.last_notified_at < now() - make_interval(secs => r.renotify_seconds)
                """);
        for (Map<String, Object> d : due) {
            long id = ((Number) d.get("id")).longValue();
            int notified = ((Number) d.get("notify_count")).intValue();
            int max = (int) jsonNum((String) d.get("params"), "notify_max", 1);
            if (notified < max) {
                notifyFired(id);   // notify_count++ · last_notified_at=now() · 아웃박스 재적재
                log.info("재발송 {} ({}/{}회)", id, notified + 1, max);
            }
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
        long startedAt = System.currentTimeMillis();
        try {
            evaluateWithCandidates(ruleType + ":all", ruleType, signalFn);
        } catch (Exception ex) {
            log.warn("알림 평가 실패({}): {}", ruleType, ex.getMessage());
            // 로그 한 줄만 남기고 넘어가면 화면에서는 아무 일도 없었던 것처럼 보인다.
            // "어제 이 규칙이 왜 안 울렸나"에 답하려면 실패가 이력으로 남아야 한다.
            recordEvalLog(ruleIdOf(ruleType + ":all"), ruleType, "FAILED", null,
                    (int) (System.currentTimeMillis() - startedAt), ex.toString());
        }
    }

    /** builtin_key 로 규칙 id 를 찾는다. 아직 시드 전이면 null(이력은 유형 코드로만 남는다). */
    private Long ruleIdOf(String builtinKey) {
        try {
            return jdbc.queryForObject(
                    "SELECT id FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    Long.class, builtinKey);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * 규칙 평가 이력 한 줄. «의미 있는 사건»만 남긴다 - 평상시 정상까지 적으면 규칙 하나당
     * 하루 4,320행이 되어 조회도 보존도 감당하기 어렵다(V55 주석 참고).
     *
     * <p>이력 기록이 실패해도 평가 자체를 막지 않는다. 감시 장치가 부수 기능 때문에 멈추면 안 된다.
     */
    private void recordEvalLog(Long ruleId, String ruleTypeCode, String result,
                               Integer matchedCount, Integer durationMs, String message) {
        try {
            jdbc.update("""
                    INSERT INTO alert_rule_eval_log
                        (rule_id, rule_type_code, result, matched_count, duration_ms, message)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    ruleId, ruleTypeCode, result, matchedCount, durationMs, clipMessage(message));
        } catch (Exception ex) {
            log.debug("규칙 평가 이력 기록 실패({}): {}", ruleTypeCode, ex.getMessage());
        }
    }

    private static String clipMessage(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 2000 ? message : message.substring(0, 2000) + "…";
    }


    /** 규칙 SELECT 마다 붙이는 스케줄 컬럼(뒤에 FROM 이 이어지므로 끝에 쉼표 대신 공백). */
    private static final String SCHEDULE_COLUMNS =
            "schedule_enabled, schedule_time, schedule_last_fired_on ";

    private static boolean scheduled(Map<String, Object> rule) {
        return Boolean.TRUE.equals(rule.get("schedule_enabled"));
    }

    /**
     * 스케줄 규칙이 «지금 점검할 차례인가». 통과하면 그 자리에서 점검 횟수를 선점한다.
     *
     * <p>스케줄이 꺼진 규칙(기본값)은 항상 true — 20초 루프에서 종전대로 상시 판정한다.
     * 켜진 규칙은 지정 시각의 1회차와, 그 뒤 «점검 간격»마다 «점검 횟수»에 이를 때까지
     * 다시 돈다. 03:00 / 5분 / 3회면 03:00 · 03:05 · 03:10 세 번 점검한다. 매번 새로
     * 판정하므로 그 사이 복구된 대상은 다음 점검에서 빠진다.
     *
     * <p>판정과 선점을 «한 문장»으로 한다. 20초마다 도는 루프라 읽고-쓰기로 나누면 그 사이
     * 여러 번 통과해 같은 점검이 연달아 돌 수 있다.
     *
     * <p>schedule_time 은 시간대 없는 time 이고 localtime 은 «세션» TZ 기준이다. pgjdbc 가
     * 접속 시 세션 TZ 를 JVM 기본값으로 맞추므로, 화면에서 고른 03:00 은 앱 기준 03:00 이 된다
     * (다른 TZ 로 붙는 psql 등에서 이 표를 직접 건드리면 어긋난다 — 기존 신호 테이블과 같은 전제).
     *
     * <p>지정 시각에 앱이 꺼져 있었다면 다음 기동 때 그날 몫을 늦게라도 돌린다(안 보내는 것보다
     * 늦게 보내는 쪽이 관제에 안전하다). 반대로 규칙을 «저장한 날»은 이미 지난 시각이 즉시
     * 발화하지 않도록 저장 시점에 오늘을 소진 처리해 둔다(AlertAdminController).
     */
    private boolean scheduleDue(Map<String, Object> rule) {
        if (!scheduled(rule)) {
            return true;
        }
        int claimed = jdbc.update("""
                UPDATE alert_rule SET
                    schedule_last_fired_on = current_date,
                    schedule_run_count = CASE
                        WHEN schedule_last_fired_on IS DISTINCT FROM current_date THEN 1
                        ELSE schedule_run_count + 1 END,
                    schedule_last_run_at = now(),
                    updated_at = now()
                WHERE id = ?
                  AND schedule_enabled
                  AND schedule_time IS NOT NULL
                  AND localtime >= schedule_time
                  AND (
                      -- 오늘 첫 점검
                      schedule_last_fired_on IS DISTINCT FROM current_date
                      -- 또는 2회차 이후: 횟수가 남았고 간격이 지났을 때
                      OR (renotify_seconds > 0
                          AND schedule_run_count < GREATEST(1, COALESCE((params_json->>'notify_max')::int, 1))
                          AND schedule_last_run_at < now() - make_interval(secs => renotify_seconds))
                  )
                """, ((Number) rule.get("id")).longValue());
        return claimed == 1;
    }

    /**
     * 스케줄 점검 회차마다 «아직 실패 중인» 대상을 다시 알린다.
     *
     * <p>2회차부터는 알림 인스턴스가 이미 열려 있어 상태 전이가 없다. 그대로 두면 03:05·03:10
     * 점검은 아무것도 보내지 않는다 — 요구는 "점검해서 실패건 있으면 발송"이므로 회차마다 보낸다.
     *
     * <p>이번 회차에 «새로 만들어져 이미 보낸» 알림은 제외한다. 판정보다 먼저 찍어 둔
     * schedule_last_run_at 보다 발송 시각이 뒤면 이번 회차에 나간 것이다.
     * 확인(ack)했거나 상위원인에 접힌(suppressed) 알림은 상시 규칙과 같은 기준으로 건너뛴다.
     */
    private void renotifyScheduledRun(long ruleId) {
        jdbc.query("""
                SELECT ai.id FROM alert_instance ai JOIN alert_rule r ON r.id = ai.rule_id
                WHERE ai.rule_id = ? AND ai.closed_at IS NULL AND ai.state = 'FIRING'
                  AND ai.ack_at IS NULL AND ai.suppressed_by IS NULL
                  AND (ai.last_notified_at IS NULL OR ai.last_notified_at < r.schedule_last_run_at)
                """, (rs, i) -> rs.getLong(1), ruleId).forEach(this::notifyFired);
    }

    /** JOB 실패(이벤트 기반). 최근 실패한 잡 실행마다 발화하고, 30분 새 실패가 없으면 해소한다. */
    private void evaluateJobRules() {
        Map<String, Object> jobRule;
        try {
            jobRule = jdbc.queryForMap(
                    "SELECT id, scope_json::text AS scope, " + SCHEDULE_COLUMNS
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL AND enabled",
                    JOB_BUILTIN_KEY);
        } catch (Exception ex) {
            return;   // 규칙 미시드/비활성
        }
        if (!scheduleDue(jobRule)) {
            return;
        }
        long ruleId = ((Number) jobRule.get("id")).longValue();
        ScopeFilter scope = scopeOf(jobRule);
        boolean daily = scheduled(jobRule);
        if (scope.isChainScoped()) {
            // (스케줄 여부는 체인 평가기 안에서 다시 본다)
            // 감시 단위가 «적재 테이블»로 지정된 규칙. 그룹 단위 run 으로는 어느 테이블이
            // 실패했는지 알 수 없어서 판정 근거 자체를 바꾼다(아래 메서드 주석 참고).
            evaluateJobRulesByChain(ruleId, scope, daily);
            return;
        }
        // ended_at 은 timestamp without time zone. 앱 세션 TZ 로 now() 와 비교되므로,
        // 신호 기록(등록기)과 이 평가기는 반드시 같은 세션 TZ(앱 JVM 기본)에서 돈다.
        // 상시 규칙은 «방금 난 실패 이벤트»를, 스케줄 규칙은 그 시각의 «현재 실패 상태»
        // (= 잡별 마지막 실행 결과가 FAILED)를 본다. 하루 한 번 보는데 10분 창을 쓰면
        // 점검 직전 10분 안에 끝난 실패만 걸려 사실상 아무것도 못 잡는다.
        List<Map<String, Object>> failed = daily
                ? jdbc.queryForList("""
                        SELECT * FROM (
                            SELECT DISTINCT ON (r.job_id)
                                   r.id AS run_id, r.job_id, j.job_name, r.status
                            FROM etl_job_run r JOIN etl_job j ON j.id = r.job_id
                            WHERE r.ended_at IS NOT NULL AND r.ended_at > now() - interval '7 days'
                            ORDER BY r.job_id, r.ended_at DESC
                        ) t WHERE t.status = 'FAILED'
                        """)
                : jdbc.queryForList("""
                        SELECT r.id AS run_id, r.job_id, j.job_name
                        FROM etl_job_run r JOIN etl_job j ON j.id = r.job_id
                        WHERE r.status = 'FAILED' AND r.ended_at > now() - interval '10 minutes'
                        ORDER BY r.ended_at DESC
                        """);
        Set<String> failingNow = new HashSet<>();
        for (Map<String, Object> f : failed) {
            long runId = ((Number) f.get("run_id")).longValue();
            long jobId = ((Number) f.get("job_id")).longValue();
            if (!scope.allows(jobId)) {
                continue;   // 감시 범위(job 선택) 밖은 건너뛴다.
            }
            String jobName = (String) f.get("job_name");
            String target = "JOB:" + jobId;
            failingNow.add(target);
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
        resolveJobInstances(ruleId, daily, failingNow);
        if (daily) {
            renotifyScheduledRun(ruleId);
        }
    }

    /**
     * JOB 규칙 해소. 상시 규칙은 «30분간 새 실패가 없으면» 닫고, 스케줄 규칙은 이번 점검에서
     * 실패로 안 잡힌 대상을 그 자리에서 닫는다 - 하루 한 번만 보는데 30분 창을 쓰면 발화 30분
     * 뒤 자동으로 닫히고 다음 날 또 열려, 실제로는 계속 실패 중인데도 «해소됨»으로 보인다.
     */
    private void resolveJobInstances(long ruleId, boolean daily, Set<String> failingNow) {
        if (daily) {
            for (Map<String, Object> open : jdbc.queryForList(
                    "SELECT id, target_key FROM alert_instance WHERE rule_id=? AND closed_at IS NULL", ruleId)) {
                if (!failingNow.contains((String) open.get("target_key"))) {
                    resolve((Number) open.get("id"), "FIRING");
                }
            }
            return;
        }
        jdbc.query("""
                SELECT id FROM alert_instance WHERE rule_id=? AND closed_at IS NULL
                  AND last_transition_at < now() - interval '30 minutes'
                """, (rs, i) -> rs.getLong(1), ruleId).forEach(id -> resolve(id, "FIRING"));
    }

    /**
     * ETL Job 실패를 «적재 테이블(체인)» 단위로 판정한다.
     *
     * <p>그룹 단위 판정은 etl_job_run 을 보는데, 그 행은 프로세스 그룹 하나에 하나뿐이라
     * "DZ 가 실패했다"까지만 알 수 있다. DZ 는 테이블 5개를 적재하므로 정작 필요한
     * "COM001M 적재만 실패"를 가려낼 수 없다. 그래서 여기서는 프로세서 단위로 남는
     * nifi_execution_log 의 ERROR 행을 근거로 쓰고, 그 프로세서가 속한 체인의 최종 적재
     * 스텝을 대표로 삼는다.
     *
     * <p>체인은 이름 규칙이 아니라 etl_job_link(실제 NiFi 연결)를 거슬러 올라가 만든다 -
     * trigger-dz-COM001M -> extract-tb-COM001M -> truncate-dz-COM001M -> load-dz-COM001M
     * 중 어디서 실패해도 같은 체인의 알림으로 모인다. 이는 ETL 관리 화면 왼쪽 트리가 job 수를
     * 세는 기준(NifiProcessGroupTreeService.JobCounts - 커넥션으로 이어진 덩어리 하나 = job
     * 하나)과 같은 단위다. DZ/DW 는 실제로 트리의 5개와 여기 5개가 정확히 일치한다.
     * 다만 DZ_UPSERT 처럼 적재 스텝이 일렬로 이어져 트리에서 한 덩어리로 보이는 플로우는
     * 여기서 테이블별로 더 잘게 쪼갠다 - 감시 단위가 트리보다 굵어지는 경우는 없다.
     */
    private void evaluateJobRulesByChain(long ruleId, ScopeFilter scope, boolean daily) {
        Map<String, Long> chainOf = chainByProcessor();
        if (chainOf.isEmpty()) {
            return;   // 미러가 아직 안 돌았거나 적재 스텝이 없다.
        }
        Map<Long, String> labels = chainLabels();

        // 상시 규칙은 방금 난 실패 이벤트를, 스케줄 규칙은 그 시각의 «현재 실패 상태»를 본다.
        // 후자는 프로세서별 «가장 최근» 기록만 남긴 뒤(DISTINCT ON) 그게 실패인 것만 고른다 -
        // 그 뒤에 성공으로 다시 돈 체인까지 매일 아침 알리면 안 된다.
        List<Map<String, Object>> failures = daily
                ? jdbc.queryForList("""
                        SELECT * FROM (
                            SELECT DISTINCT ON (e.processor_id)
                                   e.id, e.processor_id, e.job_id, j.job_name, e.status, e.level
                            FROM nifi_execution_log e LEFT JOIN etl_job j ON j.id = e.job_id
                            WHERE e.occurred_at > now() - interval '7 days'
                            ORDER BY e.processor_id, e.occurred_at DESC
                        ) t WHERE t.status = 'FAILED' AND t.level = 'ERROR'
                        """)
                : jdbc.queryForList("""
                        SELECT e.id, e.processor_id, e.job_id, j.job_name
                        FROM nifi_execution_log e LEFT JOIN etl_job j ON j.id = e.job_id
                        WHERE e.status = 'FAILED' AND e.level = 'ERROR'
                          AND e.occurred_at > now() - interval '10 minutes'
                        ORDER BY e.occurred_at DESC
                        """);
        Set<String> failingNow = new HashSet<>();
        for (Map<String, Object> f : failures) {
            String processorId = (String) f.get("processor_id");
            Long stepId = processorId == null ? null : chainOf.get(processorId);
            // 어느 적재 체인에도 안 걸리는 프로세서(적재 스텝이 없는 덩어리)는 버리지 않는다.
            // id 0 으로 두면 INCLUDE 에서는 "고르지 않은 것"이라 조용하고, EXCLUDE 에서는
            // "제외하지 않은 것"이라 그대로 알린다 - 특정 테이블만 빼려다 나머지 실패까지
            // 통째로 못 받는 구멍을 막는다.
            if (!scope.allows(stepId == null ? 0L : stepId)) {
                continue;   // 감시 범위 밖.
            }
            long logId = ((Number) f.get("id")).longValue();
            // job_id 는 nullable(LEFT JOIN 이고 컬럼 자체도 null 가능) - 미상은 한 칸에 모은다.
            Object jobId = f.get("job_id");
            Object jobName = f.get("job_name");
            String label = stepId != null
                    ? labels.getOrDefault(stepId, "적재 " + stepId)
                    : (jobName != null ? (String) jobName : "미상 ETL");
            String target = stepId != null ? "CHAIN:" + stepId
                    : "JOB:" + (jobId != null ? jobId : "unknown");
            if (!failingNow.add(target)) {
                continue;   // 한 체인의 여러 프로세서가 걸린 경우 - 알림은 체인당 하나면 된다.
            }
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
                        """, Long.class, ruleId, JOB_RULE_TYPE, target, label,
                        "ETL Job 실패: " + label, logId);
                event(id, "CREATED", null, "FIRING", "SYSTEM");
                event(id, "FIRED", null, "FIRING", "SYSTEM");
                notifyFired(id);
                log.info("JOB 실패 알림 생성(체인) {} target={}", id, label);
            } else {
                Map<String, Object> inst = open.get(0);
                Long last = inst.get("last_failed_run_id") == null ? null
                        : ((Number) inst.get("last_failed_run_id")).longValue();
                if (last == null || last != logId) {
                    Number id = (Number) inst.get("id");
                    jdbc.update("UPDATE alert_instance SET last_failed_run_id=?, fail_run_count=fail_run_count+1, "
                            + "last_evaluated_at=now(), last_transition_at=now(), updated_at=now() WHERE id=?", logId, id);
                    event(id.longValue(), "RECURRED", null, null, "SYSTEM");
                    notifyFired(id.longValue());
                }
            }
        }
        resolveJobInstances(ruleId, daily, failingNow);
        if (daily) {
            renotifyScheduledRun(ruleId);
        }
    }

    /**
     * NiFi 프로세서 id -> 그 프로세서가 속한 체인의 최종 적재 스텝 id.
     *
     * <p>적재 스텝(target_table 보유)에서 링크를 «거꾸로» 따라 올라가며 상류 프로세서를 모은다.
     * 한 프로세서가 여러 적재 스텝으로 흘러가는 구조라면 먼저 도달한 체인에 귀속시킨다
     * (지금 플로우들은 테이블별로 갈라져 있어 실제로는 겹치지 않는다).
     */
    private Map<String, Long> chainByProcessor() {
        Map<String, List<String>> upstream = new HashMap<>();
        for (Map<String, Object> l : jdbc.queryForList(
                "SELECT from_component_id, to_component_id FROM etl_job_link WHERE deleted_at IS NULL")) {
            upstream.computeIfAbsent((String) l.get("to_component_id"), k -> new ArrayList<>())
                    .add((String) l.get("from_component_id"));
        }
        List<Map<String, Object>> loadSteps = jdbc.queryForList(
                "SELECT id, nifi_processor_id FROM etl_job_step "
                        + "WHERE deleted_at IS NULL AND target_table IS NOT NULL ORDER BY id");
        // 1단계: 적재 프로세서는 «자기 자신»의 대표로 먼저 못박는다. DZ_UPSERT 처럼 적재 스텝이
        // 일렬로 이어진 플로우에서, 하류 체인의 상류 탐색이 앞 테이블의 적재 스텝을 삼키는 걸 막는다.
        Map<String, Long> chainOf = new HashMap<>();
        for (Map<String, Object> st : loadSteps) {
            chainOf.put((String) st.get("nifi_processor_id"), ((Number) st.get("id")).longValue());
        }
        // 2단계: 각 적재 스텝에서 상류로 거슬러 올라가 아직 임자 없는 프로세서만 흡수한다.
        for (Map<String, Object> st : loadSteps) {
            long stepId = ((Number) st.get("id")).longValue();
            Deque<String> queue = new ArrayDeque<>(upstream.getOrDefault((String) st.get("nifi_processor_id"), List.of()));
            while (!queue.isEmpty()) {
                String pid = queue.poll();
                if (pid == null || chainOf.putIfAbsent(pid, stepId) != null) {
                    continue;   // 이미 다른(또는 같은) 체인에 귀속됨 - 순환도 여기서 멈춘다.
                }
                queue.addAll(upstream.getOrDefault(pid, List.of()));
            }
        }
        return chainOf;
    }

    /** 적재 스텝 id -> 화면·알림에 쓸 이름("DZ / dz_com001m"). */
    private Map<Long, String> chainLabels() {
        Map<Long, String> labels = new HashMap<>();
        for (Map<String, Object> r : jdbc.queryForList(
                // 이름 규칙은 감시 범위 드롭다운(AlertAdminController.scopeTargets)과 같게 둔다 -
                // 고른 이름과 알림에 찍히는 이름이 다르면 어느 걸 고른 건지 알 수 없다.
                "SELECT s.id, j.job_name || ' / ' || s.target_table "
                        + "|| CASE WHEN count(*) OVER (PARTITION BY j.job_name, s.target_table) > 1 "
                        + "THEN ' (' || s.step_name || ')' ELSE '' END AS name "
                        + "FROM etl_job_step s JOIN etl_job j ON j.id = s.job_id "
                        + "WHERE s.deleted_at IS NULL AND s.target_table IS NOT NULL")) {
            labels.put(((Number) r.get("id")).longValue(), (String) r.get("name"));
        }
        return labels;
    }

    /** DATA 신선도(적재 정체). 24h 내 활동한 소스가 임계(분)만큼 새 입력이 없으면 발화, 재개되면 해소한다. */
    private void evaluateDataFreshness() {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, params_json::text AS params, enabled, " + SCHEDULE_COLUMNS
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL", DATA_BUILTIN_KEY);
        } catch (Exception ex) {
            return;   // 규칙 미시드
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        if (!scheduleDue(rule)) {
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
        if (scheduled(rule)) {
            renotifyScheduledRun(ruleId);
        }
    }

    /** 수집기 중단(데드맨). WatchdogService 가 연 collector_outage(미종료)를 대기열 알림으로 승격/해소한다. */
    private void evaluateCollectorOutage() {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, enabled, " + SCHEDULE_COLUMNS
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL",
                    COLLECTOR_BUILTIN_KEY);
        } catch (Exception ex) {
            return;
        }
        if (!Boolean.TRUE.equals(rule.get("enabled")) || !scheduleDue(rule)) {
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
        if (scheduled(rule)) {
            renotifyScheduledRun(ruleId);
        }
    }

    private void evaluateMemoryRule() {
        Map<String, Object> rule;
        try {
            rule = jdbc.queryForMap(
                    "SELECT id, severity, for_seconds, clear_seconds, params_json::text AS params, enabled, "
                            + SCHEDULE_COLUMNS
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL", BUILTIN_KEY);
        } catch (Exception ex) {
            return;   // 규칙이 아직 시드 안 됨
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        if (!scheduleDue(rule)) {
            return;
        }
        long ruleId = ((Number) rule.get("id")).longValue();
        // 스케줄 규칙은 하루 한 번만 돌므로 지속시간 누적(for/clear)이 영원히 안 찬다.
        // 그 시각의 상태로 즉시 판정하도록 0 으로 눌러 둔다.
        int forSec = scheduled(rule) ? 0 : ((Number) rule.get("for_seconds")).intValue();
        int clearSec = scheduled(rule) ? 0 : ((Number) rule.get("clear_seconds")).intValue();
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
        if (scheduled(rule)) {
            renotifyScheduledRun(ruleId);
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
        // 규칙 이력에도 남긴다. 발화만 있고 해소가 없으면 "아직 열려 있는지"를 화면에서 알 수 없다.
        try {
            Map<String, Object> inst = jdbc.queryForMap(
                    "SELECT rule_id, rule_type_code, target_key FROM alert_instance WHERE id=?", id);
            recordEvalLog(inst.get("rule_id") == null ? null : ((Number) inst.get("rule_id")).longValue(),
                    (String) inst.get("rule_type_code"), "RESOLVED", 1, null,
                    "조건 해제: " + inst.get("target_key"));
        } catch (Exception ex) {
            log.debug("해소 이력 기록 실패: {}", ex.getMessage());
        }
    }

    private void event(Long instanceId, String type, String from, String to, String actor) {
        jdbc.update("INSERT INTO alert_instance_event (instance_id, event_type, from_state, to_state, actor, occurred_at) "
                + "VALUES (?, ?, ?, ?, ?, now())", instanceId, type, from, to, actor);
    }

    private String summary(double memPct, double threshold) {
        return String.format("메모리 %.1f%% (임계 %.0f%%)", memPct, threshold);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 규칙 감시 범위(scope_json). ALL=전체 / INCLUDE=지정 대상만 / EXCLUDE=지정 대상 제외.
     * ids 는 JOB 규칙이면 etl_job.id, CDC 규칙이면 pipeline_definition.id.
     */
    private record ScopeFilter(String kind, Set<Long> ids, String idKind) {

        /** ids 가 etl_job_step.id(적재 체인)를 가리키는가. 기본은 etl_job.id(그룹). */
        boolean isChainScoped() {
            return "CHAIN".equals(idKind) && !"ALL".equals(kind);
        }

        boolean allows(long id) {
            if ("INCLUDE".equals(kind)) {
                return ids.isEmpty() || ids.contains(id);   // 지정 없으면 사실상 전체(미설정 방어)
            }
            if ("EXCLUDE".equals(kind)) {
                return !ids.contains(id);
            }
            return true;   // ALL
        }
    }

    /** rule 맵의 scope(scope_json::text)를 파싱. 파싱 실패/미지정은 ALL. */
    private ScopeFilter scopeOf(Map<String, Object> rule) {
        Object raw = rule.get("scope");
        String json = raw == null ? null : raw.toString();
        if (json == null || json.isBlank()) {
            return new ScopeFilter("ALL", Set.of(), "JOB");
        }
        try {
            JsonNode n = MAPPER.readTree(json);
            String kind = n.path("kind").asText("ALL");
            String idKind = n.path("idKind").asText("JOB");
            Set<Long> ids = new HashSet<>();
            if (n.has("ids") && n.get("ids").isArray()) {
                n.get("ids").forEach(x -> ids.add(x.asLong()));
            }
            // 그룹째 고른 것은 «지금» 그 아래에 있는 대상으로 펼친다. 저장은 그룹 id 로 해두고
            // 펼치는 것을 평가 시점에 하므로, 나중에 테이블이 추가돼도 규칙을 다시 저장할
            // 필요가 없다(그게 그룹 선택의 의도다).
            ids.addAll(expandGroupPgIds(n, idKind));
            ids.addAll(expandGroupConnIds(n));
            return new ScopeFilter(kind, ids, idKind);
        } catch (Exception ex) {
            return new ScopeFilter("ALL", Set.of(), "JOB");
        }
    }

    /**
     * scope_json 의 {@code groupPgIds}(NiFi 프로세스 그룹) → 그 아래 감시 단위 id 로 펼친다.
     *
     * <p>하위 그룹까지 재귀로 내려간다(DZ 를 고르면 DZ_COM·DZ_POP 아래 테이블까지). 계층은
     * ETL>관리 화면과 같은 nifi_process_group_metadata 를 쓴다.
     *
     * <p>idKind=CHAIN 이면 적재 스텝(테이블), 아니면 etl_job 이 감시 단위다.
     * 예전 규칙에는 groupPgIds 가 없으므로 빈 집합을 돌려주고 동작이 그대로다.
     */
    private Set<Long> expandGroupPgIds(JsonNode scope, String idKind) {
        if (!scope.has("groupPgIds") || !scope.get("groupPgIds").isArray()
                || scope.get("groupPgIds").isEmpty()) {
            return Set.of();
        }
        List<String> pgIds = new java.util.ArrayList<>();
        scope.get("groupPgIds").forEach(x -> pgIds.add(x.asText()));
        String placeholders = pgIds.stream().map(v -> "?")
                .collect(java.util.stream.Collectors.joining(","));
        String leafSql = "CHAIN".equals(idKind)
                ? "SELECT s.id FROM etl_job_step s JOIN etl_job j ON j.id = s.job_id"
                  + " WHERE s.deleted_at IS NULL AND j.deleted_at IS NULL AND s.target_table IS NOT NULL"
                  + " AND j.nifi_pg_id IN (SELECT process_group_id FROM d)"
                : "SELECT id FROM etl_job"
                  + " WHERE deleted_at IS NULL AND nifi_pg_id IN (SELECT process_group_id FROM d)";
        String sql = "WITH RECURSIVE d AS ("
                + " SELECT process_group_id FROM nifi_process_group_metadata"
                + " WHERE process_group_id IN (" + placeholders + ")"
                + " UNION ALL"
                + " SELECT m.process_group_id FROM nifi_process_group_metadata m"
                + " JOIN d ON m.parent_group_id = d.process_group_id) " + leafSql;
        try {
            Set<Long> out = new HashSet<>();
            jdbc.queryForList(sql, pgIds.toArray())
                    .forEach(row -> out.add(((Number) row.values().iterator().next()).longValue()));
            return out;
        } catch (Exception ex) {
            log.warn("감시 범위 그룹 펼치기 실패 - 그룹 선택분은 이번 평가에서 제외됩니다: {}",
                    ex.getMessage());
            return Set.of();
        }
    }

    /**
     * scope_json 의 {@code groupConnIds}(소스 연결정보) → 그 원천의 파이프라인 id 로 펼친다.
     *
     * <p>CDC 규칙의 «그룹째 감시». AlertAdminController.expandGroupConnIds 와 같은 규칙이다.
     */
    private Set<Long> expandGroupConnIds(JsonNode scope) {
        if (!scope.has("groupConnIds") || !scope.get("groupConnIds").isArray()
                || scope.get("groupConnIds").isEmpty()) {
            return Set.of();
        }
        List<Long> connIds = new java.util.ArrayList<>();
        scope.get("groupConnIds").forEach(x -> connIds.add(x.asLong()));
        String ph = connIds.stream().map(v -> "?").collect(java.util.stream.Collectors.joining(","));
        try {
            Set<Long> out = new HashSet<>();
            jdbc.queryForList("SELECT id FROM pipeline_definition WHERE source_connection_id IN ("
                            + ph + ")", connIds.toArray())
                    .forEach(row -> out.add(((Number) row.values().iterator().next()).longValue()));
            return out;
        } catch (Exception ex) {
            log.warn("감시 범위 연결정보 펼치기 실패 - 그룹 선택분 제외: {}", ex.getMessage());
            return Set.of();
        }
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
                    "SELECT id, severity, for_seconds, clear_seconds, params_json::text AS params, "
                            + "scope_json::text AS scope, enabled, " + SCHEDULE_COLUMNS
                            + "FROM alert_rule WHERE builtin_key = ? AND deleted_at IS NULL", builtinKey);
        } catch (Exception ex) {
            return;   // 아직 시드 안 됨
        }
        if (!Boolean.TRUE.equals(rule.get("enabled"))) {
            return;
        }
        if (!scheduleDue(rule)) {
            return;
        }
        long ruleId = ((Number) rule.get("id")).longValue();
        String severity = (String) rule.get("severity");
        // 스케줄 규칙은 그 시각 상태로 즉시 발화/해소한다(위 evaluateMemoryRule 주석 참고).
        int forSec = scheduled(rule) ? 0 : ((Number) rule.get("for_seconds")).intValue();
        int clearSec = scheduled(rule) ? 0 : ((Number) rule.get("clear_seconds")).intValue();

        long evalStartedAt = System.currentTimeMillis();
        List<Candidate> candidates = signalFn.apply(rule);
        if (candidates == null) {
            // 신호 자체를 못 읽은 경우. "모름"을 "정상"으로 칠하지 않으려면 해소도 하지 않는다.
            // 판정을 «보류»한 구간이라 화면에서 구분할 수 있어야 한다.
            recordEvalLog(ruleId, ruleType, "NO_SIGNAL", null,
                    (int) (System.currentTimeMillis() - evalStartedAt), "신호를 읽지 못해 판정을 보류했습니다.");
            return;
        }
        Set<String> firingKeys = new HashSet<>();
        for (Candidate c : candidates) {
            firingKeys.add(c.targetKey());
            advance(ruleId, ruleType, severity, forSec, c);
        }
        if (!candidates.isEmpty()) {
            recordEvalLog(ruleId, ruleType, "FIRED", candidates.size(),
                    (int) (System.currentTimeMillis() - evalStartedAt),
                    candidates.stream().map(Candidate::targetKey).limit(5)
                            .collect(java.util.stream.Collectors.joining(", "))
                            + (candidates.size() > 5 ? " 외 " + (candidates.size() - 5) : ""));
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
        if (scheduled(rule)) {
            renotifyScheduledRun(ruleId);
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
        ScopeFilter scope = scopeOf(rule);
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
            if (!scope.allows(pid)) {
                return null;
            }
            String name = rs.getString("name");
            String label = name != null ? name : ("파이프라인 " + pid);
            return new Candidate("CDC_LAG:" + pid, label, "CDC", (double) lag, threshold,
                    String.format("%s — 미처리 %,d건 (임계 %,.0f건)", label, lag, threshold),
                    "/cdc/logs");
        }).stream().filter(java.util.Objects::nonNull).toList();
    }

    /** 커넥터 FAILED(원본 규칙표, 위험). 최신 스냅샷의 소스/싱크 상태를 본다. */
    private List<Candidate> connectorFailedSignals(Map<String, Object> rule) {
        ScopeFilter scope = scopeOf(rule);
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
            if (!scope.allows(pid)) {
                return null;
            }
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

    /** N일 이상 미실행(원본 규칙표, 경고). 한 번은 돌았던 잡만 대상으로 한다. */
    private List<Candidate> notRunSignals(Map<String, Object> rule) {
        int days = (int) jsonNum(rule.get("params").toString(), "days", 7);
        ScopeFilter scope = scopeOf(rule);
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
            if (!scope.allows(jobId)) {
                return null;
            }
            int idle = rs.getInt("idle_days");
            String name = rs.getString("job_name");
            String label = name != null ? name : ("잡 " + jobId);
            return new Candidate("JOB_IDLE:" + jobId, label, "ETL", (double) idle, (double) days,
                    String.format("%s — %d일간 실행 없음", label, idle), "/airflow/dashboard");
        }, days);
    }


    /**
     * 게시된 워크플로우 목록. 게시 안 한 것은 Airflow 에 DAG 자체가 없어 감시 대상이 아니다.
     */
    private List<Map<String, Object>> publishedWorkflows() {
        return jdbc.queryForList("""
                SELECT id, workflow_key, name, schedule_cron,
                       COALESCE(dag_id_override, 'etl_wf_' || workflow_key) AS dag_id
                FROM etl_workflow
                WHERE deleted_at IS NULL AND published_at IS NOT NULL""");
    }

    /**
     * 워크플로우 실패. 게시된 워크플로우의 <b>가장 최근 DAG 실행</b>이 failed 면 후보.
     *
     * <p>잡 단위 규칙(JOB_FAILURE)과 겹치지 않는다. 저쪽은 «잡이 돌다가 실패»고 이쪽은
     * «워크플로우 전체가 실패»다 - 트리거 단계에서 죽어 잡이 하나도 안 돈 경우는 이쪽만 잡는다
     * (실측 2026-09-02 13:53, etl_job_run 에 행이 없어 어떤 규칙도 울리지 않았다).
     *
     * <p>신호는 Airflow API 를 그대로 본다(우리 DB 사본 없음) - 실시간 모니터링 화면과 같은
     * 원천이라 «화면은 실패인데 알림은 조용»한 어긋남이 생기지 않는다.
     */
    private List<Candidate> workflowFailureSignals(Map<String, Object> rule) {
        ScopeFilter scope = scopeOf(rule);
        Map<String, AirflowDagRunClient.DagRunWithDag> latest = latestDagRuns();
        List<Candidate> out = new ArrayList<>();
        for (Map<String, Object> wf : publishedWorkflows()) {
            long wfId = ((Number) wf.get("id")).longValue();
            if (!scope.allows(wfId)) {
                continue;
            }
            AirflowDagRunClient.DagRunWithDag run = latest.get(String.valueOf(wf.get("dag_id")));
            if (run == null || !"failed".equalsIgnoreCase(String.valueOf(run.state()))) {
                continue;
            }
            String label = String.valueOf(wf.get("name"));
            out.add(new Candidate("WORKFLOW_FAILED:" + wfId, label, "ETL", null, null,
                    String.format("%s — 워크플로우 실행 실패 (%s)", label, run.dagRunId()),
                    "/airflow/dashboard"));
        }
        return out;
    }

    /**
     * 워크플로우 미완료. 정기 실행인데 <b>유예 시간</b>이 지나도 성공한 실행이 없으면 후보.
     *
     * <p>«실패»가 아니라 «아예 안 돎»을 잡는다 - 스케줄이 꺼졌거나, DAG 파싱이 깨졌거나,
     * 상위가 자식 스케줄을 억제했는데 상위에 스케줄이 없는 경우 등. 이런 상황은 DAG 실행 자체가
     * 생기지 않아 실패 규칙으로는 영원히 안 걸린다.
     *
     * <p>크론을 해석해 «다음 예정 시각»을 계산하지 않는다. 크론 파서를 들이는 것보다
     * «마지막 성공이 언제였나»가 운영자가 실제로 묻는 질문에 가깝고, 유예 시간 하나로
     * 일 배치·시간 배치를 모두 표현할 수 있다.
     */
    private List<Candidate> workflowNotCompletedSignals(Map<String, Object> rule) {
        ScopeFilter scope = scopeOf(rule);
        int graceMinutes = (int) jsonNum(rule.get("params").toString(), "grace_minutes", 1440);
        Map<String, AirflowDagRunClient.DagRunWithDag> lastOk = lastSuccessfulDagRuns();
        java.time.OffsetDateTime deadline = java.time.OffsetDateTime.now().minusMinutes(graceMinutes);
        List<Candidate> out = new ArrayList<>();
        for (Map<String, Object> wf : publishedWorkflows()) {
            Object cron = wf.get("schedule_cron");
            if (cron == null || String.valueOf(cron).isBlank()) {
                continue;   // 수동 실행 전용은 «안 돌았다»가 이상이 아니다
            }
            long wfId = ((Number) wf.get("id")).longValue();
            if (!scope.allows(wfId)) {
                continue;
            }
            AirflowDagRunClient.DagRunWithDag ok = lastOk.get(String.valueOf(wf.get("dag_id")));
            boolean overdue = ok == null || ok.startDate() == null || ok.startDate().isBefore(deadline);
            if (!overdue) {
                continue;
            }
            String label = String.valueOf(wf.get("name"));
            String since = ok == null || ok.startDate() == null
                    ? "성공 이력 없음"
                    : "마지막 성공 " + ok.startDate().toLocalDateTime().toString().replace('T', ' ');
            out.add(new Candidate("WORKFLOW_OVERDUE:" + wfId, label, "ETL", null, (double) graceMinutes,
                    String.format("%s — %d분 넘게 성공한 실행이 없음 (%s)", label, graceMinutes, since),
                    "/airflow/dashboard"));
        }
        return out;
    }

    /** DAG 별 «가장 최근» 실행(시작 시각 내림차순이라 먼저 본 것이 최신). */
    private Map<String, AirflowDagRunClient.DagRunWithDag> latestDagRuns() {
        Map<String, AirflowDagRunClient.DagRunWithDag> out = new java.util.LinkedHashMap<>();
        for (AirflowDagRunClient.DagRunWithDag r : recentDagRuns()) {
            out.putIfAbsent(r.dagId(), r);
        }
        return out;
    }

    /** DAG 별 «가장 최근 성공» 실행. */
    private Map<String, AirflowDagRunClient.DagRunWithDag> lastSuccessfulDagRuns() {
        Map<String, AirflowDagRunClient.DagRunWithDag> out = new java.util.LinkedHashMap<>();
        for (AirflowDagRunClient.DagRunWithDag r : recentDagRuns()) {
            if ("success".equalsIgnoreCase(String.valueOf(r.state()))) {
                out.putIfAbsent(r.dagId(), r);
            }
        }
        return out;
    }

    /**
     * 최근 DAG 실행 목록. 규칙 둘이 같은 주기에 각자 부르면 호출이 두 배가 되므로 짧게 캐시한다.
     * 엔진 주기(20초)보다 짧게 두어 «화면보다 오래된 값»을 보지 않게 한다.
     */
    private List<AirflowDagRunClient.DagRunWithDag> recentDagRuns() {
        long now = System.currentTimeMillis();
        List<AirflowDagRunClient.DagRunWithDag> cached = dagRunCache;
        if (cached != null && now - dagRunCachedAt < DAGRUN_CACHE_MS) {
            return cached;
        }
        try {
            List<AirflowDagRunClient.DagRunWithDag> fresh = dagRunClient.getRecentDagRuns(200);
            dagRunCache = fresh;
            dagRunCachedAt = now;
            return fresh;
        } catch (Exception ex) {
            log.warn("Airflow 실행 이력 조회 실패 - 워크플로우 규칙은 이번 주기를 건너뜁니다: {}",
                    ex.getMessage());
            return List.of();
        }
    }
}
