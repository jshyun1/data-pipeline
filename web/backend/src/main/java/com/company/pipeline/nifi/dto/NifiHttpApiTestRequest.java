package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record NifiHttpApiTestRequest(
        @NotBlank
        @Pattern(regexp = "GET|POST")
        String httpMethod,

        @NotBlank
        @Size(max = 2000)
        String apiUrl,

        @Size(max = 4000)
        String apiKey
) {
}
