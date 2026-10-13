package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import com.fasterxml.jackson.databind.JsonNode;

public record NifiHttpLoadCreateRequest(
        @NotBlank
        @Size(max = 128)
        String jobName,

        @NotBlank
        String parentGroupId,

        @Size(max = 1024)
        String comments,

        @NotBlank
        @Pattern(regexp = "GET|POST")
        String httpMethod,

        @NotBlank
        @Size(max = 2000)
        String apiUrl,

        @Size(max = 4000)
        String apiKey,

        @NotBlank
        String targetServiceId,

        @NotBlank
        @Size(max = 64)
        String targetDatabaseType,

        @NotBlank
        @Size(max = 128)
        String targetSchema,

        @NotBlank
        @Size(max = 128)
        String targetTable,

        @NotBlank
        @Pattern(regexp = "INSERT|UPSERT")
        String loadMode,

        @Size(max = 20000)
        String queryRecordSql,

        @Size(max = 512)
        String primaryKeys,

        JsonNode wizardSnapshot
) {
}
