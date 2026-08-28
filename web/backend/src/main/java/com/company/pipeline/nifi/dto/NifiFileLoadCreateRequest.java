package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

public record NifiFileLoadCreateRequest(
        @NotBlank
        @Size(max = 128)
        String jobName,

        @NotBlank
        String parentGroupId,

        @Size(max = 1024)
        String comments,

        @NotBlank
        @Pattern(regexp = "csv|excel")
        String fileExtension,

        @NotBlank
        String inputDirectory,

        @NotEmpty
        List<@NotBlank @Size(max = 128) String> columns,

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
        String targetTable
) {
}
