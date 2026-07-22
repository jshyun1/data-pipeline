package com.company.pipeline.user.dto;

/** 로그인 성공 응답: JWT와 사용자 정보. */
public record LoginResponse(String token, long expiresInMinutes, UserResponse user) {
}
