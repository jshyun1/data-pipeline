package com.company.pipeline.monitoring.dto;

import java.time.LocalDate;
import java.util.List;

public record DailyLoadSummaryResponse(
        List<DailyPoint> daily,
        List<KeyedPoint> topPipelines,
        List<KeyedPoint> topTasks
) {
    public record DailyPoint(LocalDate date, long count) {
        public static DailyPoint from(DailyCountProjection projection) {
            return new DailyPoint(projection.getDate(), projection.getCount());
        }
    }

    public record KeyedPoint(String key, String label, long count) {
        public static KeyedPoint from(KeyedCountProjection projection) {
            return new KeyedPoint(projection.getKey(), projection.getLabel(), projection.getCount());
        }
    }
}
