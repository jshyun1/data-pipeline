package com.company.pipeline.user.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import javax.crypto.SecretKey;
import org.springframework.stereotype.Service;

/**
 * 로그인 성공 시 JWT를 발급하고, 이후 요청의 Bearer 토큰을 검증한다.
 * subject=userId, "admin" 클레임에 관리자 여부를 담는다.
 */
@Service
public class JwtService {

    private final SecretKey key;
    private final long expirationMinutes;

    public JwtService(JwtProperties properties) {
        this.key = Keys.hmacShaKeyFor(properties.secret().getBytes(StandardCharsets.UTF_8));
        this.expirationMinutes = properties.expirationMinutes();
    }

    public String issue(String userId, boolean admin) {
        Instant now = Instant.now();
        return Jwts.builder()
                .subject(userId)
                .claim("admin", admin)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plusSeconds(expirationMinutes * 60)))
                .signWith(key)
                .compact();
    }

    /** 유효하면 클레임을 반환, 서명 불일치/만료 등이면 예외를 던진다. */
    public Claims parse(String token) {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    public long expirationMinutes() {
        return expirationMinutes;
    }
}
