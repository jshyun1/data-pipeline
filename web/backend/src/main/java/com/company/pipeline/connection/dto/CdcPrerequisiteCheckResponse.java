package com.company.pipeline.connection.dto;

public record CdcPrerequisiteCheckResponse(
        String code,
        String label,
        String status,
        String actualValue,
        String guidance
) {
}
