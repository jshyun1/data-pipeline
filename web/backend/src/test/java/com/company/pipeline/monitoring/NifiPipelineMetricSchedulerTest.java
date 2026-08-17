package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.heartbeat.HeartbeatService;
import com.company.pipeline.jobcatalog.JobLookup;
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
    private JobLookup jobLookup;
    @Mock
    private NifiClient nifiClient;
    @Mock
    private NifiCounterSnapshotRepository snapshotRepository;
    @Mock
    private PipelineDailyLoadMetricService dailyLoadMetricService;
    @Mock
    private NifiExecutionLogEntryRepository executionLogRepository;
    @Mock
    private HeartbeatService heartbeat;

    private NifiPipelineMetricScheduler scheduler;

    @Test
    void checkCounters_deltaDetected_incrementsDailyMetricAndSavesExecutionLogRow() {
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        String processorId = "59d9d764-019f-1000-bc10-355d39a9b2fd";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "PutDatabaseRecord",
                "PutDatabaseRecord", 0);
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
    void checkCounters_upsertCounter_isCountedLikeInsertCounter() {
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        String processorId = "a7d8266c-aa51-32df-641b-9545e403b408";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "PutDatabaseRecord",
                "PutDatabaseRecord", 0);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "d4bbc12a-019f-1000-ad1e-afcab3d438dd", "수입테이블4개컬럼매핑",
                List.of(new NifiFlowStatusResponse.ProcessorStatusEntry(processor)), List.of());
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        when(nifiClient.getRootFlowStatus())
                .thenReturn(new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate)));

        var counter = new NifiCountersResponse.Counter("c1", "PutDatabaseRecord (" + processorId + ")",
                "UPSERT updates performed", 200L);
        when(nifiClient.getCounters()).thenReturn(new NifiCountersResponse(new NifiCountersResponse.Counters(
                new NifiCountersResponse.AggregateSnapshot(List.of(counter)))));
        when(snapshotRepository.findById(processorId))
                .thenReturn(Optional.of(new NifiCounterSnapshot(processorId, "PutDatabaseRecord", 50L)));

        scheduler.checkCounters();

        verify(dailyLoadMetricService).incrementLoadedCount(eq("NIFI"), eq(processorId), any(),
                eq("PutDatabaseRecord"), eq(150L));

        ArgumentCaptor<NifiExecutionLogEntry> captor = ArgumentCaptor.forClass(NifiExecutionLogEntry.class);
        verify(executionLogRepository).save(captor.capture());
        assertThat(captor.getValue().getGroupName()).isEqualTo("수입테이블4개컬럼매핑");
        assertThat(captor.getValue().getInsertedCount()).isEqualTo(150L);
    }

    @Test
    void checkCounters_noDelta_doesNotSaveExecutionLogRow() {
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        String processorId = "59d9d764-019f-1000-bc10-355d39a9b2fd";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "PutDatabaseRecord",
                "PutDatabaseRecord", 0);
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

    @Test
    void checkCounters_counterClearedByRestart_resetsSnapshotSoNextLoadIsNotSwallowed() {
        // NiFi를 재시작하면 카운터가 통째로 사라진다(프로세서는 그대로). 이때 예전
        // 기준점을 남겨두면, TRUNCATE 후 전량 재적재해서 카운터가 예전과 똑같은 값까지
        // 올라왔을 때 delta가 0이 되어 적재가 로그에 한 줄도 안 남는다(실측으로 확인).
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        String processorId = "9e2dd726-019f-1000-338b-b3f02d4d9673";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "load-dz-POP002L",
                "PutDatabaseRecord", 0);
        var processorEntry = new NifiFlowStatusResponse.ProcessorStatusEntry(processor);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "9e2da75d-019f-1000-1f21-9e2ab492cf11", "DZ", List.of(processorEntry), List.of());
        var groupEntry = new NifiFlowStatusResponse.ProcessGroupStatusEntry(group);
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(List.of(), List.of(groupEntry));
        var flow = new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate));
        when(nifiClient.getRootFlowStatus()).thenReturn(flow);

        // 재시작 직후: 이 프로세서의 카운터가 응답에 아예 없다.
        var counters = new NifiCountersResponse(
                new NifiCountersResponse.Counters(new NifiCountersResponse.AggregateSnapshot(List.of())));
        when(nifiClient.getCounters()).thenReturn(counters);

        when(snapshotRepository.findById(processorId))
                .thenReturn(Optional.of(new NifiCounterSnapshot(processorId, "load-dz-POP002L", 4_305_006L)));

        scheduler.checkCounters();

        ArgumentCaptor<NifiCounterSnapshot> captor = ArgumentCaptor.forClass(NifiCounterSnapshot.class);
        verify(snapshotRepository).save(captor.capture());
        assertThat(captor.getValue().getLastValue()).isZero();
        // 이 주기 자체는 적재가 없었으므로 로그도 남기지 않는다.
        verify(executionLogRepository, never()).save(any());
    }

    @Test
    void checkCounters_executeGroovyScriptLoader_isCountedLikePutDatabaseRecord() {
        // 비정형 그룹의 이미지/동영상 적재는 PutDatabaseRecord가 아니라
        // ExecuteGroovyScript다(바이너리를 레코드로 넣으면 파일 전체가 힙에 올라와서).
        // 스크립트가 session.adjustCounter()로 같은 카운터를 올리므로 집계 대상이어야 한다.
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        String processorId = "a7d8b6b3-019f-1000-199b-2fe36157b6b2";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "store-image-binary",
                "ExecuteGroovyScript", 0);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "a7d8b5ea-019f-1000-7126-f9de54751340", "unstructured-image",
                List.of(new NifiFlowStatusResponse.ProcessorStatusEntry(processor)), List.of());
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        when(nifiClient.getRootFlowStatus())
                .thenReturn(new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate)));

        var counter = new NifiCountersResponse.Counter("c1", "store-image-binary (" + processorId + ")",
                "INSERT updates performed", 5L);
        when(nifiClient.getCounters()).thenReturn(new NifiCountersResponse(new NifiCountersResponse.Counters(
                new NifiCountersResponse.AggregateSnapshot(List.of(counter)))));
        when(snapshotRepository.findById(processorId))
                .thenReturn(Optional.of(new NifiCounterSnapshot(processorId, "store-image-binary", 3L)));

        scheduler.checkCounters();

        verify(dailyLoadMetricService).incrementLoadedCount(eq("NIFI"), eq(processorId), any(),
                eq("store-image-binary"), eq(2L));

        ArgumentCaptor<NifiExecutionLogEntry> captor = ArgumentCaptor.forClass(NifiExecutionLogEntry.class);
        verify(executionLogRepository).save(captor.capture());
        assertThat(captor.getValue().getGroupName()).isEqualTo("unstructured-image");
        assertThat(captor.getValue().getInsertedCount()).isEqualTo(2L);
    }

    @Test
    void checkCounters_processorTypeThatNeverLoads_isIgnored() {
        // 타입 필터를 넓힌 뒤에도 아무 프로세서나 집계하지 않는다는 것.
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        var processor = new NifiFlowStatusResponse.ProcessorStatus("p1", "parse-csv-and-tag", "UpdateRecord", 0);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "g1", "unstructured-csv",
                List.of(new NifiFlowStatusResponse.ProcessorStatusEntry(processor)), List.of());
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        when(nifiClient.getRootFlowStatus())
                .thenReturn(new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate)));
        when(nifiClient.getCounters()).thenReturn(new NifiCountersResponse(
                new NifiCountersResponse.Counters(new NifiCountersResponse.AggregateSnapshot(List.of()))));

        scheduler.checkCounters();

        verify(snapshotRepository, never()).findById(any());
        verify(snapshotRepository, never()).save(any());
        verify(executionLogRepository, never()).save(any());
    }

    @Test
    void checkCounters_counterAbsentAndSnapshotAlreadyZero_doesNotRewriteSnapshot() {
        // 아직 한 번도 안 돈 프로세서까지 매 주기 DB에 쓰지 않도록.
        scheduler = new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup, heartbeat);

        String processorId = "9e2dd726-019f-1000-338b-b3f02d4d9673";
        var processor = new NifiFlowStatusResponse.ProcessorStatus(processorId, "load-dz-POP002L",
                "PutDatabaseRecord", 0);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "9e2da75d-019f-1000-1f21-9e2ab492cf11", "DZ",
                List.of(new NifiFlowStatusResponse.ProcessorStatusEntry(processor)), List.of());
        var rootAggregate = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        when(nifiClient.getRootFlowStatus())
                .thenReturn(new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(rootAggregate)));
        when(nifiClient.getCounters()).thenReturn(new NifiCountersResponse(
                new NifiCountersResponse.Counters(new NifiCountersResponse.AggregateSnapshot(List.of()))));
        when(snapshotRepository.findById(processorId))
                .thenReturn(Optional.of(new NifiCounterSnapshot(processorId, "load-dz-POP002L", 0L)));

        scheduler.checkCounters();

        verify(snapshotRepository, never()).save(any());
    }
}
