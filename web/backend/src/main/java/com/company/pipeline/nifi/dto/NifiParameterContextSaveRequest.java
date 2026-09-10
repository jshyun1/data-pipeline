package com.company.pipeline.nifi.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record NifiParameterContextSaveRequest(
        @NotBlank String name,
        String description,
        @NotNull List<@Valid ParameterRequest> parameters
) {

    public record ParameterRequest(
            @NotBlank String name,
            String value,
            Boolean sensitive,
            String description
    ) {
    }
}
