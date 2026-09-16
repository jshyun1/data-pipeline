package com.company.pipeline.llm.dto;

/**
 * 연결 테스트 결과.
 *
 * @param status     SUCCESS | AUTH_FAILED | FORBIDDEN | NOT_FOUND | RATE_LIMITED | BAD_REQUEST |
 *                   PROVIDER_ERROR | TIMEOUT | UNREACHABLE | INVALID_CONFIG
 * @param message    화면에 그대로 띄울 한국어 안내
 * @param httpStatus 제공자가 준 HTTP 코드(연결 자체가 안 됐으면 null)
 * @param detail     제공자 응답 본문 앞부분 - 원인 확인용. 키가 담기지 않는 응답 본문만 넣는다.
 */
public record LlmTestResult(
        String status,
        boolean success,
        String message,
        Integer httpStatus,
        long durationMs,
        String detail) {
}
