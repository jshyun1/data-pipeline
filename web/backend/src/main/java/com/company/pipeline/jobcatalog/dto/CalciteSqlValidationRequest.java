package com.company.pipeline.jobcatalog.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CalciteSqlValidationRequest(
        @NotBlank
        @Size(max = 20000)
        String sql
) {
}
