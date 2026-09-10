package com.company.pipeline.connection.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record SqlValidationRequest(
        @NotBlank
        @Size(max = 20000)
        String sql
) {
}
