package com.company.pipeline.nifi.dto;

public record NifiProcessGroupResponse(
        String id,
        String name,
        String parentGroupId,
        Integer processorCount
) {
}
