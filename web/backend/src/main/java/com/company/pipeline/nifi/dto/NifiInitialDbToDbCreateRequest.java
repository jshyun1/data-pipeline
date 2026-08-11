package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record NifiInitialDbToDbCreateRequest(
        @NotBlank
        @Size(max = 128)
        String jobName,

        @NotBlank
        String parentGroupId,

        @Size(max = 1024)
        String comments,

        @NotBlank
        String sourceServiceId,

        @NotBlank
        @Size(max = 64)
        String sourceDatabaseType,

        @NotBlank
        @Size(max = 128)
        String sourceSchema,

        @NotBlank
        @Size(max = 128)
        String sourceTable,

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
        @Pattern(regexp = "INSERT")
        String loadMode
) {
}
