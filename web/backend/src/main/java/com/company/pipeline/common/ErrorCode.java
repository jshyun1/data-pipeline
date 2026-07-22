package com.company.pipeline.common;

import org.springframework.http.HttpStatus;

public enum ErrorCode {
    VALIDATION_ERROR(HttpStatus.BAD_REQUEST, "요청 값이 올바르지 않습니다."),
    UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "인증이 필요합니다."),
    INVALID_CREDENTIALS(HttpStatus.UNAUTHORIZED, "아이디 또는 비밀번호가 올바르지 않습니다."),
    ACCOUNT_NOT_APPROVED(HttpStatus.FORBIDDEN, "승인 대기 중이거나 사용할 수 없는 계정입니다."),
    USER_ALREADY_EXISTS(HttpStatus.CONFLICT, "이미 존재하는 아이디입니다."),
    CONNECTION_NOT_FOUND(HttpStatus.NOT_FOUND, "연결정보를 찾을 수 없습니다."),
    PIPELINE_NOT_FOUND(HttpStatus.NOT_FOUND, "파이프라인을 찾을 수 없습니다."),
    KAFKA_CONNECT_ERROR(HttpStatus.BAD_GATEWAY, "Kafka Connect 요청 처리 중 오류가 발생했습니다."),
    NIFI_ERROR(HttpStatus.BAD_GATEWAY, "NiFi 요청 처리 중 오류가 발생했습니다."),
    FILEBEAT_CONFIG_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Filebeat 설정 파일 처리 중 오류가 발생했습니다."),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "서버 내부 오류가 발생했습니다.");

    private final HttpStatus httpStatus;
    private final String defaultMessage;

    ErrorCode(HttpStatus httpStatus, String defaultMessage) {
        this.httpStatus = httpStatus;
        this.defaultMessage = defaultMessage;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }
}
