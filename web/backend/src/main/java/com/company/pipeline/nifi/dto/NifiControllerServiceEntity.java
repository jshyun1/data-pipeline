package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiControllerServiceEntity(String id, Revision revision, Component component) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Revision(Long version) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Component(String id, String name, String type, String state) {
    }
}
