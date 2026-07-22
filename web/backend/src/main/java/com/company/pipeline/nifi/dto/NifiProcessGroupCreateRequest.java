package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record NifiProcessGroupCreateRequest(
        @NotBlank
        @Size(max = 128)
        String name
) {
}
