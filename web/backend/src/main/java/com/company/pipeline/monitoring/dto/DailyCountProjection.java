package com.company.pipeline.monitoring.dto;

import java.time.LocalDate;

public interface DailyCountProjection {
    LocalDate getDate();

    Long getCount();
}
