package com.company.pipeline.user.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * JWT 서명/만료 설정. secret은 기본값 없음 - 없으면 부팅 실패(하드코딩 키 방지,
 * pipeline.crypto.secret과 동일한 fail-fast 원칙). 최소 32바이트 이상 권장.
 */
@ConfigurationProperties(prefix = "pipeline.jwt")
public record JwtProperties(String secret, long expirationMinutes) {
    public JwtProperties {
        if (expirationMinutes <= 0) {
            expirationMinutes = 480; // 기본 8시간
        }
    }
}
