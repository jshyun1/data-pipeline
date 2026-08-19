package com.company.pipeline.connection.dto;

public record ConnectionReferenceResponse(
        String referenceType,
        Long referenceId,
        String name,
        String role,
        String status
) {
}
