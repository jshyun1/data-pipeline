package com.company.pipeline.common;

import org.springframework.http.HttpStatus;

public enum ErrorCode {
    VALIDATION_ERROR(HttpStatus.BAD_REQUEST, "요청 값이 올바르지 않습니다."),
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
