package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import com.fasterxml.jackson.databind.JsonNode;

public record NifiUnstructuredLoadCreateRequest(
        @NotBlank
        @Size(max = 128)
        String jobName,

        @NotBlank
        String parentGroupId,

        @Size(max = 1024)
        String comments,

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

        JsonNode wizardSnapshot
) {
}
