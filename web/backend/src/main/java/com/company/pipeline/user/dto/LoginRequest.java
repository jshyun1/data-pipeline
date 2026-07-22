package com.company.pipeline.user.dto;

import jakarta.validation.constraints.NotBlank;

public record LoginRequest(
        @NotBlank(message = "아이디를 입력하세요") String userId,
        @NotBlank(message = "비밀번호를 입력하세요") String password) {
}
