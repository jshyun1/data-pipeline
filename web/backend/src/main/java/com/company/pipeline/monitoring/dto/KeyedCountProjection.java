package com.company.pipeline.monitoring.dto;

public interface KeyedCountProjection {
    String getKey();

    String getLabel();

    Long getCount();
}
