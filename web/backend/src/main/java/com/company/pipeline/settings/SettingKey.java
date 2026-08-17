package com.company.pipeline.settings;

import static com.company.pipeline.settings.SettingValueType.BYTES;
import static com.company.pipeline.settings.SettingValueType.DECIMAL;
import static com.company.pipeline.settings.SettingValueType.INT;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 운영 파라미터(임계값·주기·보존기간)의 코드 기본값 단일 원천 (설계서 4-2).
 *
 * <p>app_setting 테이블은 "고객이 실제로 바꾼 값"만 담고(override-only), 행이 없으면 여기
 * defaultValue 가 쓰인다. min/max 는 화면/PUT 검증용이며, 없으면(null) 범위 검증을 생략한다.
 *
 * <p>D-5 결정에 따라 metrics.exclude.label.patterns(테스트 데이터 제외) 키는 두지 않는다 -
 * PERF 잔여물은 고객사에 없고 env 태깅/제외 기능 자체를 만들지 않기로 했다(WORK_LOG §19.1).
 */
public enum SettingKey {
    // ── 서버 리소스 (사고 2: 메모리 76.8%에서 무경고) ─────────────────────────
    RESOURCE_MEMORY_WARN_PERCENT("resource.memory.warn.percent", INT, "80", 50.0, 92.0),
    RESOURCE_MEMORY_CRIT_PERCENT("resource.memory.crit.percent", INT, "90", 50.0, 92.0),
    RESOURCE_MEMORY_CLEAR_PERCENT("resource.memory.clear.percent", INT, "72", 40.0, 88.0),
    RESOURCE_MEMORY_MIN_AVAILABLE("resource.memory.min.available", BYTES, "2147483648", 536870912.0, 8589934592.0),
    RESOURCE_MEMORY_SWAP_WINDOW_MIN("resource.memory.swap.window.min", INT, "15", 5.0, 60.0),
    RESOURCE_MEMORY_SWAP_DELTA("resource.memory.swap.delta", BYTES, "104857600", 10485760.0, 1073741824.0),
    RESOURCE_CPU_LOAD_PER_CORE("resource.cpu.load.per.core", DECIMAL, "2.0", 0.5, 8.0),
    RESOURCE_DISK_WARN_PERCENT("resource.disk.warn.percent", INT, "85", 50.0, 95.0),
    RESOURCE_DISK_CRIT_PERCENT("resource.disk.crit.percent", INT, "90", 50.0, 95.0),
    // 설계서가 min/max를 생략(...)한 항목 - 범위 검증 없이 둔다.
    RESOURCE_DISK_MIN_FREE("resource.disk.min.free", BYTES, "10737418240", null, null),
    // 절대 여유 임계는 재정의 없으면 total x ratio 로 파생(6-6-5절).
    RESOURCE_MEMORY_MIN_AVAIL_PCT("resource.memory.min.available.percent", INT, "18", 5.0, 40.0),
    RESOURCE_DISK_MIN_FREE_PCT("resource.disk.min.free.percent", INT, "4", 1.0, 20.0),
    // 메모리 압박(/proc/pressure/memory full avg60). 실측 평시 0.18.
    RESOURCE_MEM_PSI_WARN("resource.mem.psi.warn", DECIMAL, "5.0", 0.5, 100.0),
    RESOURCE_MEM_PSI_CRIT("resource.mem.psi.crit", DECIMAL, "20.0", 1.0, 100.0),
    // 추세 투영(재기동 직후 표본 부족으로 인한 오탐 방지).
    RESOURCE_TREND_MINUTES("resource.trend.minutes", INT, "240", 30.0, 10080.0),
    RESOURCE_TREND_TARGET_PERCENT("resource.trend.target.percent", INT, "90", 60.0, 99.0),
    RESOURCE_TREND_MIN_SAMPLES("resource.trend.min.samples", INT, "15", 5.0, 720.0),
    RESOURCE_TREND_MIN_R2("resource.trend.min.r2", DECIMAL, "0.75", 0.3, 0.99),
    RESOURCE_TREND_BOOT_GRACE_SEC("resource.trend.boot.grace.sec", INT, "900", 60.0, 7200.0),
    // 컨테이너 cgroup 상한 대비(상한에 붙으면 OOMKill 이라 호스트보다 높게).
    RESOURCE_CONTAINER_WARN_PCT("resource.container.warn.percent", INT, "85", 50.0, 98.0),
    RESOURCE_CONTAINER_CRIT_PCT("resource.container.crit.percent", INT, "95", 50.0, 99.0),
    // 관측 자체의 파라미터.
    RESOURCE_SAMPLE_INTERVAL_SEC("resource.sample.interval.sec", INT, "60", 30.0, 600.0),
    RESOURCE_MAX_SERIES_P1("resource.max.series.p1", INT, "50", 5.0, 500.0),
    RESOURCE_VOLUME_WALK_DEADLINE_S("resource.volume.walk.deadline.s", INT, "10", 2.0, 120.0),

    // ── 데이터 흐름 (사고 1: 커넥터 RUNNING, 지표 0) ─────────────────────────
    // lag 절대값이 아니라 해소 예상 시간으로 판정한다(6-3절).
    FLOW_LAG_ETA_WARN_SECONDS("flow.lag.eta.warn.seconds", INT, "600", 60.0, 86400.0),
    FLOW_LAG_WARN_ROWS("flow.lag.warn.rows", INT, "50000", 100.0, 100000000.0),
    FLOW_STALL_CONFIRM_SECONDS("flow.stall.confirm.seconds", INT, "120", 60.0, 3600.0),
    FLOW_STALLED_AFTER_SECONDS("flow.stalled.after.seconds", INT, "1800", 60.0, 86400.0),

    // ── 수집기 신선도 (기존 ProcessHealthService 3분/10분 규약 계승) ─────────
    HEALTH_STALE_SECONDS_NIFI("health.stale.seconds.nifi", INT, "180", 60.0, 3600.0),
    HEALTH_DEAD_SECONDS_NIFI("health.dead.seconds.nifi", INT, "600", 120.0, 86400.0),
    HEALTH_STALE_SECONDS_KAFKA("health.stale.seconds.kafka", INT, "180", 60.0, 3600.0),
    HEALTH_DEAD_SECONDS_KAFKA("health.dead.seconds.kafka", INT, "600", 120.0, 86400.0),

    // ── 알림 엔진 ────────────────────────────────────────────────────────────
    ALERT_EVAL_INTERVAL_SECONDS("alert.eval.interval.seconds", INT, "20", 10.0, 600.0),
    ALERT_PASS_BUDGET_MS("alert.pass.budget.ms", INT, "10000", 1000.0, 60000.0),
    ALERT_QUERY_TIMEOUT_MS("alert.query.timeout.ms", INT, "3000", 500.0, 30000.0),
    ALERT_BOOT_GRACE_SECONDS("alert.boot.grace.seconds", INT, "40", 0.0, 900.0),

    // ── 발송 (조용시간 키는 두지 않는다 - R3) ────────────────────────────────
    NOTIFY_RATE_CRITICAL_PER_MIN("notify.rate.critical.per.min", INT, "10", 1.0, 120.0),
    NOTIFY_RATE_OTHER_PER_MIN("notify.rate.other.per.min", INT, "10", 1.0, 120.0),
    NOTIFY_RECIPIENT_RATE_PER_HOUR("notify.recipient.rate.per.hour", INT, "10", 1.0, 200.0),
    NOTIFY_BATCH_SECONDS("notify.batch.seconds", INT, "300", 30.0, 3600.0),
    NOTIFY_MAX_ATTEMPTS("notify.max.attempts", INT, "5", 1.0, 10.0),
    NOTIFY_DIGEST_HOUR("notify.digest.hour", INT, "9", 0.0, 23.0),

    // ── 보존·로그인 정책 ─────────────────────────────────────────────────────
    RETENTION_RUN_BUDGET_MINUTES("retention.run.budget.minutes", INT, "20", 1.0, 120.0),
    // 로컬 인증(U3)의 실패 잠금 임계. 하드코딩하지 않는 것이 원칙 D다.
    AUTH_LOGIN_FAIL_LOCK_COUNT("auth.login.fail.lock.count", INT, "5", 3.0, 20.0),
    AUTH_LOGIN_LOCK_MINUTES("auth.login.lock.minutes", INT, "10", 1.0, 1440.0);

    private final String key;
    private final SettingValueType type;
    private final String defaultValue;
    private final Double min;   // nullable - 범위 검증 생략
    private final Double max;   // nullable

    SettingKey(String key, SettingValueType type, String defaultValue, Double min, Double max) {
        this.key = key;
        this.type = type;
        this.defaultValue = defaultValue;
        this.min = min;
        this.max = max;
    }

    private static final Map<String, SettingKey> BY_KEY =
            Stream.of(values()).collect(Collectors.toMap(SettingKey::key, Function.identity()));

    public static SettingKey fromKey(String key) {
        return BY_KEY.get(key);
    }

    public String key() {
        return key;
    }

    public SettingValueType type() {
        return type;
    }

    public String defaultValue() {
        return defaultValue;
    }

    public Double min() {
        return min;
    }

    public Double max() {
        return max;
    }
}
