package com.company.pipeline.monitoring.dto;

import java.time.LocalDate;

public interface HourlyCountProjection {
    LocalDate getDate();

    Integer getHour();

    Long getCount();
}
