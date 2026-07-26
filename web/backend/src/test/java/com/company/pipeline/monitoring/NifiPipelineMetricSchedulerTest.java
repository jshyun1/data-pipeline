package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NifiPipelineMetricSchedulerTest {

    @Mock
    private NifiClient nifiClient;
    @Mock
    private NifiCounterSnapshotRepository snapshotRepository;
    @Mock
    private PipelineDailyLoadMetricService dailyLoadMetricService;
    @Mock
    private NifiExecutionLogEntryRepository executionLogRepository;

    private NifiPipelineMetricScheduler scheduler;

    @Test
    void checkCounters_deltaDetected_incrementsDailyMetricAndSavesExecutionLogRow() {
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository);

        String processorId = "59d9d764-019f-1000-bc10-355d39a9b2fd";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "PutDatabaseRecord",
                "PutDatabaseRecord");
        var processorEntry = new NifiFlowStatusResponse.ProcessorStatusEntry(processor);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "59d6f6f7-019f-1000-3a91-49b23eaf89a1", "logfile", List.of(processorEntry), List.of());
        var groupEntry = new NifiFlowStatusResponse.ProcessGroupStatusEntry(group);
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(List.of(), List.of(groupEntry));
        var processGroupStatus = new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate);
        var flow = new NifiFlowStatusResponse(processGroupStatus);
        when(nifiClient.getRootFlowStatus()).thenReturn(flow);

        var counter = new NifiCountersResponse.Counter("c1", "Put-PutDatabaseRecord (" + processorId + ")",
                "INSERT updates performed", 150L);
        var countersAggregate = new NifiCountersResponse.AggregateSnapshot(List.of(counter));
        var counters = new NifiCountersResponse(new NifiCountersResponse.Counters(countersAggregate));
        when(nifiClient.getCounters()).thenReturn(counters);

        when(snapshotRepository.findById(processorId))
                .thenReturn(Optional.of(new NifiCounterSnapshot(processorId, "PutDatabaseRecord", 100L)));

        scheduler.checkCounters();

        verify(dailyLoadMetricService).incrementLoadedCount(eq("NIFI"), eq(processorId), any(), eq("PutDatabaseRecord"),
                eq(50L));

        ArgumentCaptor<NifiExecutionLogEntry> captor = ArgumentCaptor.forClass(NifiExecutionLogEntry.class);
        verify(executionLogRepository).save(captor.capture());
        NifiExecutionLogEntry saved = captor.getValue();
        assertThat(saved.getProcessorId()).isEqualTo(processorId);
        assertThat(saved.getProcessorName()).isEqualTo("PutDatabaseRecord");
        assertThat(saved.getGroupId()).isEqualTo("59d6f6f7-019f-1000-3a91-49b23eaf89a1");
        assertThat(saved.getGroupName()).isEqualTo("logfile");
        assertThat(saved.getInsertedCount()).isEqualTo(50L);
        assertThat(saved.getStatus()).isEqualTo(NifiExecutionLogEntry.STATUS_SUCCESS);
    }

    @Test
    void checkCounters_noDelta_doesNotSaveExecutionLogRow() {
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository);

        String processorId = "59d9d764-019f-1000-bc10-355d39a9b2fd";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "PutDatabaseRecord",
                "PutDatabaseRecord");
        var processorEntry = new NifiFlowStatusResponse.ProcessorStatusEntry(processor);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "59d6f6f7-019f-1000-3a91-49b23eaf89a1", "logfile", List.of(processorEntry), List.of());
        var groupEntry = new NifiFlowStatusResponse.ProcessGroupStatusEntry(group);
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(List.of(), List.of(groupEntry));
        var processGroupStatus = new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate);
        var flow = new NifiFlowStatusResponse(processGroupStatus);
        when(nifiClient.getRootFlowStatus()).thenReturn(flow);

        var counter = new NifiCountersResponse.Counter("c1", "Put-PutDatabaseRecord (" + processorId + ")",
                "INSERT updates performed", 100L);
        var countersAggregate = new NifiCountersResponse.AggregateSnapshot(List.of(counter));
        var counters = new NifiCountersResponse(new NifiCountersResponse.Counters(countersAggregate));
        when(nifiClient.getCounters()).thenReturn(counters);

        when(snapshotRepository.findById(processorId))
                .thenReturn(Optional.of(new NifiCounterSnapshot(processorId, "PutDatabaseRecord", 100L)));

        scheduler.checkCounters();

        verify(executionLogRepository, never()).save(any());
    }
}
