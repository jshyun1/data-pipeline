package com.company.pipeline.nifi.dto;

public record NifiHttpApiTestResponse(
        int statusCode,
        String responseBody,
        String contentType,
        String finalUrl
) {
}
