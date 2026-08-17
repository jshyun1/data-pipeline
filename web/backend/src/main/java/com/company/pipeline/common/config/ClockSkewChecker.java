package com.company.pipeline.common.config;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 부팅 시 DB(now())와 JVM 시각의 실제 차이를 검사한다 (설계서 3-3 #6, U1).
 *
 * <p>앱은 KST, metadata-db 컨테이너는 TZ 미설정 시 UTC로 뜬다. 그러나 여기서 비교하는 것은
 * "표시 타임존"이 아니라 절대 시각(Instant)이다 — now()를 timestamptz로 받아 Instant로 바꾸면
 * TZ 표시 차이는 상쇄되고, NTP 미동기 같은 진짜 시계 어긋남만 남는다. 60초를 넘으면 하트비트·
 * 보존·롤업의 경과시간 판정이 통째로 밀리므로 WARN으로 표면화한다.
 *
 * <p>(자가진단 화면 상시 노출 CLOCK_SKEW 항목은 U5에서 이 검사 결과를 읽어 표시한다. 여기서는
 * 부팅 로그로만 남긴다 — 검사가 예외를 던져 기동을 막지 않는다.)
 */
@Component
public class ClockSkewChecker {

    private static final Logger log = LoggerFactory.getLogger(ClockSkewChecker.class);
    private static final long SKEW_THRESHOLD_SECONDS = 60;

    private final JdbcTemplate jdbcTemplate;

    // 기본 DataSource는 @Primary인 metadata-db(Postgres)다(account-db(MySQL)가 아니다).
    public ClockSkewChecker(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void checkClockSkew() {
        try {
            Timestamp dbTs = jdbcTemplate.queryForObject("SELECT now()", Timestamp.class);
            if (dbTs == null) {
                log.warn("시계 정합 검사: DB now()가 null로 반환됨(검사 생략).");
                return;
            }
            long skewSeconds = Math.abs(Duration.between(dbTs.toInstant(), Instant.now()).getSeconds());
            if (skewSeconds > SKEW_THRESHOLD_SECONDS) {
                log.warn("CLOCK_SKEW: DB와 JVM 시각 차이 {}초(임계 {}초 초과). 하트비트/보존/롤업 판정이"
                        + " 밀릴 수 있음 - 호스트 NTP 동기화를 확인할 것.", skewSeconds, SKEW_THRESHOLD_SECONDS);
            } else {
                log.info("시계 정합 확인: DB와 JVM 절대시각 차이 {}초(임계 {}초).", skewSeconds, SKEW_THRESHOLD_SECONDS);
            }
        } catch (Exception ex) {
            log.warn("시계 정합 검사 실패(기동은 계속): {}", ex.getMessage());
        }
    }
}
