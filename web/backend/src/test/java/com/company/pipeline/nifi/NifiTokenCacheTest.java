package com.company.pipeline.nifi;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NifiTokenCacheTest {

    private static final Instant T0 = Instant.parse("2026-10-06T00:00:00Z");

    /** 테스트가 시간을 옮길 수 있는 시계. */
    static final class MovableClock extends Clock {
        Instant now = T0;

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    static String jwt(Instant expiry, String marker) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        return enc.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8)) + "."
                + enc.encodeToString(("{\"sub\":\"" + marker + "\",\"exp\":" + expiry.getEpochSecond() + "}")
                        .getBytes(StandardCharsets.UTF_8))
                + ".sig";
    }

    @Test
    void 만료_5분_전까지는_받아_둔_토큰을_다시_쓴다() {
        MovableClock clock = new MovableClock();
        AtomicInteger issued = new AtomicInteger();
        NifiTokenCache cache = new NifiTokenCache(
                () -> jwt(clock.now.plus(Duration.ofHours(8)), "t" + issued.incrementAndGet()), clock);

        String first = cache.get();
        clock.now = T0.plus(Duration.ofHours(7)).plus(Duration.ofMinutes(54));
        assertThat(cache.get()).isSameAs(first);
        assertThat(issued).hasValue(1);

        clock.now = T0.plus(Duration.ofHours(7)).plus(Duration.ofMinutes(55));
        assertThat(cache.get()).isNotEqualTo(first);
        assertThat(issued).hasValue(2);
    }

    @Test
    void 거절된_토큰만_버린다() {
        MovableClock clock = new MovableClock();
        AtomicInteger issued = new AtomicInteger();
        NifiTokenCache cache = new NifiTokenCache(
                () -> jwt(clock.now.plus(Duration.ofHours(8)), "t" + issued.incrementAndGet()), clock);

        String first = cache.get();
        cache.invalidate("다른 스레드가 이미 바꾼 옛 토큰");
        assertThat(cache.get()).isSameAs(first);

        cache.invalidate(first);
        assertThat(cache.get()).isNotEqualTo(first);
        assertThat(issued).hasValue(2);
    }

    @Test
    void exp_를_못_읽으면_10분만_쓴다() {
        MovableClock clock = new MovableClock();
        AtomicInteger issued = new AtomicInteger();
        NifiTokenCache cache = new NifiTokenCache(() -> "not-a-jwt-" + issued.incrementAndGet(), clock);

        cache.get();
        clock.now = T0.plus(Duration.ofMinutes(4));
        cache.get();
        assertThat(issued).hasValue(1);

        // 10분짜리라 RENEW_BEFORE(5분)보다 남은 시간의 절반(5분)이 갱신 시점이다.
        clock.now = T0.plus(Duration.ofMinutes(5));
        cache.get();
        assertThat(issued).hasValue(2);
    }
}
