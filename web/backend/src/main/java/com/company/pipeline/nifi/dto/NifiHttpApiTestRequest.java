package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record NifiHttpApiTestRequest(
        @NotBlank
        @Pattern(regexp = "GET|POST")
        String httpMethod,

        @Pattern(regexp = "JSON_API|FILE_DOWNLOAD")
        String testType,

        @NotBlank
        @Size(max = 2000)
        String apiUrl,

        @Size(max = 4000)
        String apiKey
) {
}
