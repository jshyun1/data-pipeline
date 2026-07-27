package com.company.pipeline.user.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import javax.crypto.SecretKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * cerebroetl-ui 자체 로그인용 JWT 발급/검증. Keycloak을 제거하면서 이 앱이 직접 서명하는
 * 자체 토큰으로 전환했다(dw.cloud.auth.2026의 방식과 동일한 패턴 - HMAC 대칭키 서명).
 */
@Component
public class PipelineJwtService {

    private final SecretKey key;
    private final long expirationMillis;

    public PipelineJwtService(
            @Value("${pipeline.jwt.secret}") String secret,
            @Value("${pipeline.jwt.expiration}") long expirationMillis) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        this.expirationMillis = expirationMillis;
    }

    public String issue(String userId, String userNm, String email) {
        Date expiration = new Date(System.currentTimeMillis() + expirationMillis);
        return Jwts.builder()
                .subject(userId)
                .claim("userNm", userNm != null ? userNm : "")
                .claim("email", email != null ? email : "")
                .expiration(expiration)
                .signWith(key)
                .compact();
    }

    /** 서명/만료가 유효하지 않으면 예외를 던진다. */
    public Claims parse(String token) throws JwtException {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
