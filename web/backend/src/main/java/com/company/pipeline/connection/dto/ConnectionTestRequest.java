package com.company.pipeline.connection.dto;

import com.company.pipeline.connection.DbType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** 저장 전 연결 테스트에 사용하는 평문 입력. 응답이나 DB에는 비밀번호를 포함하지 않는다. */
public record ConnectionTestRequest(
        @NotNull DbType dbType,
        @NotBlank String host,
        @NotNull @Min(1) @Max(65535) Integer port,
        String databaseName,
        String serviceName,
        @NotBlank String username,
        @NotBlank String password
) {
}
