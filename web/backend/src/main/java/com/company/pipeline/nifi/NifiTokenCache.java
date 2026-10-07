package com.company.pipeline.nifi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Supplier;

/**
 * NiFi 접근 토큰(JWT)을 받아 두고 다시 쓴다(2026-10-06).
 *
 * <p>예전에는 NiFi API 를 부를 때마다 {@code /nifi-api/access/token} 으로 토큰을 새로 받았다. NiFi single-user 로그인은
 * 비밀번호를 일부러 느린 해시로 확인해서 한 번에 CPU 를 약 0.4초 쓴다(로컬 실측 평균 423ms). 30초마다 그룹을 하나씩 훑는
 * 트리 캐시 등으로 1분에 약 45번(로컬) · 130~260번(MSA) 발급돼 NiFi CPU 가 몇 초씩 100% 로 튀었고, 상태 확인 API
 * ({@code /api/infra/processes}) 도 토큰 두 번에 0.9초가 걸렸다.
 *
 * <ul>
 *   <li>토큰의 {@code exp} 를 읽어 만료 {@link #RENEW_BEFORE} 전부터는 새로 받는다(NiFi 기본 유효 8시간).</li>
 *   <li>여럿이 동시에 원하면 한 곳만 받고 나머지는 그 토큰을 쓴다(synchronized) - 만료 순간 발급이 몰리지 않는다.</li>
 *   <li>만료 전이라도 NiFi 가 401 로 거절하면 {@link #invalidate} 로 버리고 다시 받는다({@link NifiAuthRetryInterceptor}).</li>
 *   <li>토큰은 메모리에만 둔다(DB · 로그에 남기지 않는다). {@code exp} 를 읽지 못하면 {@link #FALLBACK_TTL} 만 쓴다.</li>
 * </ul>
 */
final class NifiTokenCache {

    static final Duration RENEW_BEFORE = Duration.ofMinutes(5);
    static final Duration FALLBACK_TTL = Duration.ofMinutes(10);

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Supplier<String> issuer;
    private final Clock clock;
    private String token;
    private Instant renewAt = Instant.MIN;

    NifiTokenCache(Supplier<String> issuer, Clock clock) {
        this.issuer = issuer;
        this.clock = clock;
    }

    synchronized String get() {
        Instant now = clock.instant();
        if (token == null || !now.isBefore(renewAt)) {
            String issued = issuer.get();
            Instant expiry = expiryOf(issued, now);
            // 유효 시간이 RENEW_BEFORE 보다 짧은 토큰이면 매번 새로 받게 되므로, 그때는 남은 시간의 절반에서 갱신한다.
            Instant early = expiry.minus(RENEW_BEFORE);
            Instant half = now.plus(Duration.between(now, expiry).dividedBy(2));
            token = issued;
            renewAt = early.isAfter(half) ? early : half;
        }
        return token;
    }

    /** NiFi 가 거절한 토큰을 버린다. 그 사이 다른 스레드가 이미 새로 받았으면(다른 토큰이면) 그대로 둔다. */
    synchronized void invalidate(String rejected) {
        if (rejected != null && rejected.equals(token)) {
            token = null;
        }
    }

    static Instant expiryOf(String jwt, Instant now) {
        try {
            String[] parts = jwt.split("\\.");
            JsonNode claims = JSON.readTree(new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
            if (claims.hasNonNull("exp")) {
                return Instant.ofEpochSecond(claims.get("exp").asLong());
            }
        } catch (RuntimeException | java.io.IOException ignored) {
            // JWT 가 아니거나 exp 가 없으면 아래 보수적 기본값을 쓴다.
        }
        return now.plus(FALLBACK_TTL);
    }
}
