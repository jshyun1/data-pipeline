package com.company.pipeline.settings;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * 운영 파라미터 조회 (설계서 4-2, U2).
 *
 * <p><b>설정 조회는 절대 실패하지 않는다.</b> 조회가 예외를 던지면 그 예외가 알림 평가 루프를
 * 죽인다 - "설정을 못 읽어서 알림이 안 갔다"는 상용 제품에서 용납되지 않는다. 3단 폴백:
 * <pre>
 *   ① 인메모리 캐시 히트(TTL 30초)          → 반환
 *   ② 캐시 미스 → app_setting 전체 SELECT   → 캐시 갱신 후 반환
 *   ③ DB 조회 실패
 *        ├─ 이전에 로드한 캐시가 있으면 그 값(만료됐어도) + WARN 1회
 *        └─ 캐시조차 없으면 SettingKey.defaultValue
 * </pre>
 * TTL 30초는 평가 주기(20초)보다 길지만, 화면에서 임계값을 바꾸면 최대 2주기 안에 반영된다.
 */
@Service
public class SettingService {

    private static final Logger log = LoggerFactory.getLogger(SettingService.class);
    private static final Duration TTL = Duration.ofSeconds(30);

    private final JdbcTemplate jdbc;

    // 전체 override 를 통째로 캐시하고 단일 타임스탬프로 만료를 본다(키별 쿼리 폭증 방지).
    private volatile Map<String, String> overrides = Map.of();
    private volatile Instant loadedAt = Instant.EPOCH;
    private volatile boolean everLoaded = false;
    private final AtomicBoolean staleWarned = new AtomicBoolean(false);

    // 기본 DataSource는 @Primary인 metadata-db(Postgres)다(account-db(MySQL)가 아니다).
    public SettingService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------- getters

    public int getInt(SettingKey key) {
        return (int) parseLongOrDefault(key);
    }

    public long getLong(SettingKey key) {
        return parseLongOrDefault(key);
    }

    /** BYTES 타입도 long 으로 저장되므로 getLong 과 동일하게 처리된다. */
    public long getBytes(SettingKey key) {
        return parseLongOrDefault(key);
    }

    public BigDecimal getDecimal(SettingKey key) {
        String raw = resolve(key);
        try {
            return new BigDecimal(raw.trim());
        } catch (NumberFormatException ex) {
            log.warn("설정 {} 값 파싱 실패(‘{}’) - 기본값 사용", key.key(), raw);
            return new BigDecimal(key.defaultValue().trim());
        }
    }

    public boolean getBoolean(SettingKey key) {
        return Boolean.parseBoolean(resolve(key).trim());
    }

    public String getString(SettingKey key) {
        return resolve(key);
    }

    private long parseLongOrDefault(SettingKey key) {
        String raw = resolve(key);
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ex) {
            // 기본값은 카탈로그가 보장하는 유효값이라 이 파싱은 실패하지 않는다.
            log.warn("설정 {} 값 파싱 실패(‘{}’) - 기본값 {} 사용", key.key(), raw, key.defaultValue());
            return Long.parseLong(key.defaultValue().trim());
        }
    }

    // ---------------------------------------------------------------- resolve

    /** 재정의가 있으면 그 값, 없으면 코드 기본값. 어떤 경우에도 예외를 던지지 않는다. */
    private String resolve(SettingKey key) {
        refreshIfNeeded();
        String v = overrides.get(key.key());
        return v != null ? v : key.defaultValue();
    }

    private boolean expired() {
        return Duration.between(loadedAt, Instant.now()).compareTo(TTL) >= 0;
    }

    private void refreshIfNeeded() {
        if (everLoaded && !expired()) {
            return;
        }
        synchronized (this) {
            if (everLoaded && !expired()) {   // 잠금 획득 후 재확인
                return;
            }
            try {
                Map<String, String> fresh = new HashMap<>();
                jdbc.query("SELECT setting_key, setting_value FROM app_setting", rs -> {
                    fresh.put(rs.getString(1), rs.getString(2));
                });
                overrides = fresh;
                loadedAt = Instant.now();
                everLoaded = true;
                staleWarned.set(false);
            } catch (Exception ex) {
                // loadedAt 을 갱신하지 않아 다음 호출에서 다시 시도한다.
                if (staleWarned.compareAndSet(false, true)) {
                    log.warn("app_setting 조회 실패 - {} 진행: {}",
                            everLoaded ? "만료된 캐시로" : "코드 기본값으로", ex.getMessage());
                }
            }
        }
    }

    /** 화면/PUT 검증·재검증이 최신 override 를 보도록 강제 리로드한다. */
    public void invalidateCache() {
        loadedAt = Instant.EPOCH;
    }

    // ----------------------------------------------- 기동 시 상호관계 재검증

    /**
     * override-only 구조에서는 한쪽만 재정의된 상태가 정상이라, 업그레이드로 기본값이 바뀌면
     * warn/crit 관계가 조용히 역전될 수 있다(저장 행위가 없어 어떤 검증도 안 돈다). 부팅 시
     * 실효값 기준으로 재검증하되 <b>부팅을 막지 않고</b> 안전한 방향으로 자동 보정(UPSERT)하고
     * WARN(SETTING_AUTO_CORRECT)을 남긴다. 자가진단 상시 표시는 U5에서 이 결과를 읽어 연결한다.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void revalidateOnBoot() {
        int corrected = 0;
        corrected += ensureLessThan(SettingKey.RESOURCE_MEMORY_WARN_PERCENT, SettingKey.RESOURCE_MEMORY_CRIT_PERCENT);
        corrected += ensureLessThan(SettingKey.RESOURCE_DISK_WARN_PERCENT, SettingKey.RESOURCE_DISK_CRIT_PERCENT);
        corrected += ensureLessThan(SettingKey.RESOURCE_CONTAINER_WARN_PCT, SettingKey.RESOURCE_CONTAINER_CRIT_PCT);
        corrected += ensureLessThan(SettingKey.RESOURCE_MEM_PSI_WARN, SettingKey.RESOURCE_MEM_PSI_CRIT);
        corrected += ensureLessThan(SettingKey.HEALTH_STALE_SECONDS_NIFI, SettingKey.HEALTH_DEAD_SECONDS_NIFI);
        corrected += ensureLessThan(SettingKey.HEALTH_STALE_SECONDS_KAFKA, SettingKey.HEALTH_DEAD_SECONDS_KAFKA);
        if (corrected > 0) {
            log.warn("SETTING_AUTO_CORRECT: 업그레이드로 임계 관계가 깨져 {}건을 안전한 방향으로 보정했다.", corrected);
        } else {
            log.info("설정 상호관계 재검증 통과(보정 0건).");
        }
    }

    /** warn >= crit 이면 warn 을 crit-1(또는 crit*0.99)로 낮춰 UPSERT 한다. 실패해도 부팅은 계속. */
    private int ensureLessThan(SettingKey warnKey, SettingKey critKey) {
        try {
            BigDecimal warn = getDecimal(warnKey);
            BigDecimal crit = getDecimal(critKey);
            if (warn.compareTo(crit) < 0) {
                return 0;
            }
            boolean integral = warnKey.type() == SettingValueType.INT;
            BigDecimal fixed = integral
                    ? crit.subtract(BigDecimal.ONE)
                    : crit.multiply(new BigDecimal("0.99"));
            String value = integral ? String.valueOf(fixed.longValue()) : fixed.toPlainString();
            jdbc.update("""
                    INSERT INTO app_setting (setting_key, setting_value, value_type, version, updated_by)
                    VALUES (?, ?, ?, 1, 'system:auto-correct')
                    ON CONFLICT (setting_key) DO UPDATE
                       SET setting_value = EXCLUDED.setting_value,
                           version = app_setting.version + 1,
                           updated_at = now(),
                           updated_by = 'system:auto-correct'
                    """, warnKey.key(), value, warnKey.type().name());
            invalidateCache();
            log.warn("SETTING_AUTO_CORRECT: {}({}) >= {}({}) 역전 감지 → {}={} 로 보정.",
                    warnKey.key(), warn, critKey.key(), crit, warnKey.key(), value);
            return 1;
        } catch (Exception ex) {
            log.warn("설정 재검증 중 {} 보정 실패(부팅은 계속): {}", warnKey.key(), ex.getMessage());
            return 0;
        }
    }

    // ------------------------------------------------------------- 관리 API

    public record SettingRow(String key, String type, String effectiveValue, String defaultValue,
                             boolean overridden, Long version, String updatedBy, Double min, Double max) {}

    private static final String UPSERT_SQL = """
            INSERT INTO app_setting (setting_key, setting_value, value_type, version, updated_by)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT (setting_key) DO UPDATE
               SET setting_value = EXCLUDED.setting_value,
                   version = app_setting.version + 1,
                   updated_at = now(),
                   updated_by = EXCLUDED.updated_by
            """;

    /** 전 카탈로그 키를 실효값(+재정의 메타)과 함께 반환한다. */
    public List<SettingRow> listAll() {
        Map<String, Object[]> rows = new HashMap<>();
        try {
            jdbc.query("SELECT setting_key, setting_value, version, updated_by FROM app_setting", rs -> {
                rows.put(rs.getString(1),
                        new Object[] {rs.getString(2), rs.getLong(3), rs.getString(4)});
            });
        } catch (Exception ex) {
            log.warn("설정 목록 조회 실패(기본값으로 표시): {}", ex.getMessage());
        }
        List<SettingRow> out = new ArrayList<>();
        for (SettingKey k : SettingKey.values()) {
            Object[] r = rows.get(k.key());
            boolean overridden = r != null;
            out.add(new SettingRow(k.key(), k.type().name(),
                    overridden ? (String) r[0] : k.defaultValue(), k.defaultValue(),
                    overridden, overridden ? (Long) r[1] : null, overridden ? (String) r[2] : null,
                    k.min(), k.max()));
        }
        return out;
    }

    /** 재정의 저장(타입·범위 검증 후 UPSERT). updatedBy 는 호출자(JWT subject). */
    public void applyChange(String keyStr, String value, String updatedBy) {
        applyAll(Map.of(keyStr, value), updatedBy);
    }

    /**
     * 벌크 저장. 검증을 먼저 전부 통과시킨 뒤에만 쓴다 - 하나라도 타입/범위 위반이면
     * 아무것도 쓰지 않는다(부분 저장으로 warn/crit 한쪽만 바뀌는 사고 방지).
     */
    public void applyAll(Map<String, String> changes, String updatedBy) {
        Map<SettingKey, String> resolved = new HashMap<>();
        changes.forEach((keyStr, value) -> {
            SettingKey key = requireKey(keyStr);
            validate(key, value);
            resolved.put(key, value);
        });
        resolved.forEach((key, value) ->
                jdbc.update(UPSERT_SQL, key.key(), value, key.type().name(), updatedBy));
        invalidateCache();
    }

    /** 재정의 제거 = 기본값으로 초기화(DELETE 한 줄). */
    public void reset(String keyStr) {
        SettingKey key = requireKey(keyStr);
        jdbc.update("DELETE FROM app_setting WHERE setting_key = ?", key.key());
        invalidateCache();
    }

    private SettingKey requireKey(String keyStr) {
        SettingKey key = SettingKey.fromKey(keyStr);
        if (key == null) {
            throw new IllegalArgumentException("알 수 없는 설정 키: " + keyStr);
        }
        return key;
    }

    private void validate(SettingKey key, String value) {
        if (value == null) {
            throw new IllegalArgumentException(key.key() + ": 값이 없습니다.");
        }
        switch (key.type()) {
            case INT, BYTES, DURATION_SECONDS -> checkRange(key, parseLong(key, value));
            case DECIMAL -> checkRange(key, parseDouble(key, value));
            case BOOLEAN -> {
                String t = value.trim();
                if (!"true".equalsIgnoreCase(t) && !"false".equalsIgnoreCase(t)) {
                    throw new IllegalArgumentException(key.key() + ": BOOLEAN 이 아닙니다('" + value + "').");
                }
            }
            case STRING, JSON -> {
                // DB 는 타입만 잡는다 - 자유값. (JSON 구조 검증은 소비자 책임)
            }
        }
    }

    private double parseLong(SettingKey key, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(key.key() + ": 정수가 아닙니다('" + value + "').");
        }
    }

    private double parseDouble(SettingKey key, String value) {
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(key.key() + ": 숫자가 아닙니다('" + value + "').");
        }
    }

    private void checkRange(SettingKey key, double v) {
        if (key.min() != null && v < key.min()) {
            throw new IllegalArgumentException(key.key() + ": 최소 " + key.min() + " 이상이어야 합니다(입력 " + v + ").");
        }
        if (key.max() != null && v > key.max()) {
            throw new IllegalArgumentException(key.key() + ": 최대 " + key.max() + " 이하여야 합니다(입력 " + v + ").");
        }
    }
}
