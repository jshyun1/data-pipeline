package com.company.pipeline.connection.dto;

import com.company.pipeline.connection.DbType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * password가 null/미포함이면 기존 encrypted_password를 그대로 유지한다
 * (ConnectionService.update 참고). 비밀번호만 바꾸는 별도 엔드포인트로 분리할지는
 * 다음 증분에서 재검토.
 */
public record ConnectionUpdateRequest(
        @NotBlank String name,
        @NotNull DbType dbType,
        @NotBlank String host,
        @NotNull @Min(1) @Max(65535) Integer port,
        String databaseName,
        String serviceName,
        String schemaName,
        @NotBlank String username,
        String password
) {
}
