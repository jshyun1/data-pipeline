package com.company.pipeline.nifi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record NifiProcessorEditLockRequest(
        @NotBlank @Size(max = 64) String ownerToken,
        @Size(max = 255) String processorName
) {
}
