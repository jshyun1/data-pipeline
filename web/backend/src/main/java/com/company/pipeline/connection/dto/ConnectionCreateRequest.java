package com.company.pipeline.connection.dto;

import com.company.pipeline.connection.DbType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ConnectionCreateRequest(
        @NotBlank String name,
        @NotNull DbType dbType,
        @NotBlank String host,
        @NotNull @Min(1) @Max(65535) Integer port,
        String databaseName,
        String serviceName,
        String schemaName,
        @NotBlank String username,
        @NotBlank String password
) {
}
