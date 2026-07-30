package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.monitoring.dto.HourlyCountProjection;
import com.company.pipeline.monitoring.dto.HourlyLoadSummaryResponse.HourlyPoint;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HourlyLoadMetricServiceTest {

    @Mock
    private NifiExecutionLogEntryRepository executionLogRepository;
    @Mock
    private PipelineMetricSnapshotRepository snapshotRepository;

    @InjectMocks
    private HourlyLoadMetricService service;

    @Test
    void getHourly_nifi_returnsDateAndHourPointsWithinRequestedRange() {
        when(executionLogRepository.findHourlyInsertedTotals(
                eq(LocalDateTime.of(2026, 7, 24, 0, 0)), eq(LocalDateTime.of(2026, 7, 31, 0, 0))))
                .thenReturn(List.of(
                        projection(LocalDate.of(2026, 7, 28), 9, 1_200L),
                        projection(LocalDate.of(2026, 7, 28), 14, 800L)));

        var response = service.getHourly(LocalDate.of(2026, 7, 24), LocalDate.of(2026, 7, 30), "NIFI");

        assertThat(response.hourly()).containsExactly(
                new HourlyPoint(LocalDate.of(2026, 7, 28), 9, 1_200L),
                new HourlyPoint(LocalDate.of(2026, 7, 28), 14, 800L));
        // CDC 쪽은 건드리지 않는다(차트가 엔진별로 따로 그려진다).
        verify(snapshotRepository, never()).findHourlyCommittedDeltas(any(), any());
    }

    @Test
    void getHourly_kafka_readsOffsetDeltaSource() {
        when(snapshotRepository.findHourlyCommittedDeltas(
                eq(LocalDateTime.of(2026, 7, 30, 0, 0)), eq(LocalDateTime.of(2026, 7, 31, 0, 0))))
                .thenReturn(List.of(projection(LocalDate.of(2026, 7, 30), 3, 55L)));

        var response = service.getHourly(LocalDate.of(2026, 7, 30), LocalDate.of(2026, 7, 30), "kafka");

        assertThat(response.hourly()).containsExactly(new HourlyPoint(LocalDate.of(2026, 7, 30), 3, 55L));
        verify(executionLogRepository, never()).findHourlyInsertedTotals(any(), any());
    }

    @Test
    void getHourly_withoutSource_mergesBothEnginesSortedByDateThenHour() {
        when(executionLogRepository.findHourlyInsertedTotals(any(), any()))
                .thenReturn(List.of(projection(LocalDate.of(2026, 7, 30), 5, 100L)));
        when(snapshotRepository.findHourlyCommittedDeltas(any(), any()))
                .thenReturn(List.of(
                        projection(LocalDate.of(2026, 7, 29), 6, 3L),
                        projection(LocalDate.of(2026, 7, 30), 1, 7L)));

        var response = service.getHourly(LocalDate.of(2026, 7, 29), LocalDate.of(2026, 7, 30), null);

        assertThat(response.hourly()).extracting(HourlyPoint::hour).containsExactly(6, 1, 5);
        assertThat(response.hourly()).extracting(HourlyPoint::date)
                .containsExactly(LocalDate.of(2026, 7, 29), LocalDate.of(2026, 7, 30), LocalDate.of(2026, 7, 30));
    }

    @Test
    void getHourly_ignoresRowsWithNullOrOutOfRangeValues() {
        when(executionLogRepository.findHourlyInsertedTotals(any(), any()))
                .thenReturn(List.of(
                        projection(null, 10, 10L),
                        projection(LocalDate.of(2026, 7, 30), null, 10L),
                        projection(LocalDate.of(2026, 7, 30), 24, 10L),
                        projection(LocalDate.of(2026, 7, 30), 7, 42L)));

        var response = service.getHourly(LocalDate.of(2026, 7, 30), LocalDate.of(2026, 7, 30), "NIFI");

        assertThat(response.hourly()).containsExactly(new HourlyPoint(LocalDate.of(2026, 7, 30), 7, 42L));
    }

    private static HourlyCountProjection projection(LocalDate date, Integer hour, Long count) {
        return new HourlyCountProjection() {
            @Override
            public LocalDate getDate() {
                return date;
            }

            @Override
            public Integer getHour() {
                return hour;
            }

            @Override
            public Long getCount() {
                return count;
            }
        };
    }
}
