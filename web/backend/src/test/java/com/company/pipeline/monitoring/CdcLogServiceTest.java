package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.monitoring.dto.CdcProcessingLogResponse;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CdcLogServiceTest {

    @Mock
    private PipelineDefinitionRepository pipelineRepository;

    @Mock
    private PipelineMetricSnapshotRepository snapshotRepository;

    @Mock
    private PipelineCommandHistoryRepository commandHistoryRepository;

    private CdcLogService service;

    @BeforeEach
    void setUp() {
        service = new CdcLogService(pipelineRepository, snapshotRepository, commandHistoryRepository);
    }

    @Test
    void processingLogs_aggregatesSnapshotsByMinuteAndCalculatesOffsetDelta() {
        PipelineDefinition pipeline = pipeline(1L);
        LocalDate day = LocalDate.of(2026, 7, 27);
        LocalDateTime from = day.atStartOfDay();
        LocalDateTime to = day.plusDays(1).atStartOfDay().minusNanos(1);
        PipelineMetricSnapshot baseline = snapshot(1L, from.minusSeconds(20), 100L, 0L, "RUNNING", "RUNNING");
        PipelineMetricSnapshot firstInMinute = snapshot(1L, day.atTime(10, 0, 20), 103L, 0L, "RUNNING", "RUNNING");
        PipelineMetricSnapshot lastInMinute = snapshot(1L, day.atTime(10, 0, 40), 105L, 0L, "RUNNING", "RUNNING");
        PipelineMetricSnapshot nextMinute = snapshot(1L, day.atTime(10, 1, 20), 110L, 2L, "RUNNING", "RUNNING");

        when(pipelineRepository.findAll()).thenReturn(List.of(pipeline));
        when(snapshotRepository.findByPipelineIdInAndCollectedAtBetweenOrderByCollectedAtAsc(
                List.of(1L), from, to)).thenReturn(List.of(firstInMinute, lastInMinute, nextMinute));
        when(snapshotRepository.findTopByPipelineIdAndCollectedAtBeforeOrderByCollectedAtDesc(1L, from))
                .thenReturn(Optional.of(baseline));

        List<CdcProcessingLogResponse> result = service.processingLogs(day, day);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).occurredAt()).isEqualTo(day.atTime(10, 1));
        assertThat(result.get(0).processedCount()).isEqualTo(5L);
        assertThat(result.get(0).consumerLag()).isEqualTo(2L);
        assertThat(result.get(1).processedCount()).isEqualTo(5L);
    }

    @Test
    void processingLogs_treatsStoppedSinkAsWaitingInsteadOfFailure() {
        PipelineDefinition pipeline = pipeline(1L);
        LocalDate day = LocalDate.of(2026, 7, 27);
        LocalDateTime from = day.atStartOfDay();
        LocalDateTime to = day.plusDays(1).atStartOfDay().minusNanos(1);
        PipelineMetricSnapshot snapshot =
                snapshot(1L, day.atTime(12, 0), 120L, 20L, "RUNNING", "STOPPED");

        when(pipelineRepository.findAll()).thenReturn(List.of(pipeline));
        when(snapshotRepository.findByPipelineIdInAndCollectedAtBetweenOrderByCollectedAtAsc(
                List.of(1L), from, to)).thenReturn(List.of(snapshot));
        when(snapshotRepository.findTopByPipelineIdAndCollectedAtBeforeOrderByCollectedAtDesc(1L, from))
                .thenReturn(Optional.empty());

        CdcProcessingLogResponse response = service.processingLogs(day, day).getFirst();

        assertThat(response.status()).isEqualTo("STOPPED");
        assertThat(response.message()).contains("Kafka에 대기");
    }

    @Test
    void processingLogs_rejectsRangesLongerThan31Days() {
        LocalDate from = LocalDate.of(2026, 1, 1);

        assertThatThrownBy(() -> service.processingLogs(from, from.plusDays(32)))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("최대 31일");
    }

    private PipelineDefinition pipeline(Long id) {
        PipelineDefinition pipeline = new PipelineDefinition(
                "customers-cdc",
                "TABLE_CDC",
                1L,
                2L,
                DbType.ORACLE,
                DbType.POSTGRESQL,
                "APP",
                "CUSTOMERS",
                "public",
                "customers",
                "oracle-cdc.APP.CUSTOMERS",
                true,
                null,
                "tester");
        pipeline.setId(id);
        return pipeline;
    }

    private PipelineMetricSnapshot snapshot(
            Long pipelineId,
            LocalDateTime collectedAt,
            Long committedOffset,
            Long lag,
            String sourceState,
            String sinkState) {
        PipelineMetricSnapshot snapshot = new PipelineMetricSnapshot();
        snapshot.setPipelineId(pipelineId);
        snapshot.setCollectedAt(collectedAt);
        snapshot.setTopicName("oracle-cdc.APP.CUSTOMERS");
        snapshot.setCommittedOffset(committedOffset);
        snapshot.setConsumerLag(lag);
        snapshot.setSourceConnectorState(sourceState);
        snapshot.setSinkConnectorState(sinkState);
        return snapshot;
    }
}
