package com.company.pipeline.llm.nifi;

import jakarta.validation.constraints.NotBlank;

public record NifiPropertyHelpRequest(
        String processorType,
        @NotBlank String propertyName,
        String propertyValue,
        @NotBlank String description,
        String expressionLanguageScope,
        Boolean sensitive,
        Boolean identifiesControllerService
) {
}
