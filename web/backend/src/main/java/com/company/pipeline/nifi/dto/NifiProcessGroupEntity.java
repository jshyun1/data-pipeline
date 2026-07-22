package com.company.pipeline.nifi.dto;

public record NifiProcessGroupEntity(
        String id,
        Component component
) {

    public record Component(
            String id,
            String name,
            String parentGroupId
    ) {
    }
}
