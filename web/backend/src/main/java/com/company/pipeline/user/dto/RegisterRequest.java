package com.company.pipeline.user.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * 등록신청. 관리자 승인 전까지 use_yn='N'으로 생성된다.
 * 본부/직함 코드는 app_user 컬럼상 NOT NULL이라 필수로 받는다.
 */
public record RegisterRequest(
        @NotBlank(message = "아이디를 입력하세요") String userId,
        @NotBlank(message = "이름을 입력하세요") String userNm,
        @NotBlank(message = "비밀번호를 입력하세요") String password,
        String email,
        String telNo,
        @NotBlank(message = "본부 코드를 입력하세요") String hqCd,
        @NotBlank(message = "직함 코드를 입력하세요") String positionCd) {
}
