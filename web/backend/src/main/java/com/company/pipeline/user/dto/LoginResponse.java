package com.company.pipeline.user.dto;

public record LoginResponse(String token, UserResponse user) {
}
