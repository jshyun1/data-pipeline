package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.jobcatalog.EtlJobRunService;
import com.company.pipeline.jobcatalog.JobLookup;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import com.company.pipeline.nifi.dto.NifiProcessorDetailResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 적재 "실행 구간"이 제대로 열리고 닫히는지.
 *
 * <p>회귀 방지 대상: 60초 주기로 시점만 기록하던 시절엔 6분짜리 적재 하나가 7행으로
 * 흩어져서 소요시간도 처리량도 알 수 없었다(화면이 29페이지가 됐다). 구간이 잠깐의
 * 공백에서 쪼개지지 않는 것이 이 클래스의 핵심이다.
 */
@ExtendWith(MockitoExtension.class)
class NifiProcessorRunTrackerTest {

    private static final String PROCESSOR_ID = "9e2dd726-019f-1000-338b-b3f02d4d9673";
    private static final String GROUP_ID = "9e2da75d-019f-1000-1f21-9e2ab492cf11";
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    @Mock
    private JobLookup jobLookup;
    @Mock
    private EtlJobRunService jobRunService;
    @Mock
    private NifiClient nifiClient;
    @Mock
    private NifiProcessorRunRepository runRepository;
    @Mock
    private com.company.pipeline.heartbeat.HeartbeatService heartbeat;

    private Instant now;

    @BeforeEach
    void setUp() {
        now = Instant.parse("2026-07-29T00:00:00Z");
        lenient().when(nifiClient.getProcessor(anyString())).thenReturn(
                new NifiProcessorDetailResponse(new NifiProcessorDetailResponse.Component(
                        PROCESSOR_ID, "load-dz-POP003L", "PutDatabaseRecord",
                        new NifiProcessorDetailResponse.Config(Map.of(
                                "put-db-record-schema-name", "public",
                                "put-db-record-table-name", "dz_pop003l")))));
    }

    private NifiProcessorRunTracker tracker() {
        return new NifiProcessorRunTracker(nifiClient, runRepository, jobLookup, jobRunService, heartbeat, Clock.fixed(now, ZONE));
    }

    private LocalDateTime at() {
        return LocalDateTime.ofInstant(now, ZONE);
    }

    private void nifiReports(int activeThreads, Long counterValue) {
        nifiReports(activeThreads, counterValue, "INSERT updates performed");
    }

    private void nifiReports(int activeThreads, Long counterValue, String counterName) {
        var processor = new NifiFlowStatusResponse.ProcessorStatus(
                PROCESSOR_ID, "load-dz-POP003L", "PutDatabaseRecord", activeThreads);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                GROUP_ID, "DZ", List.of(new NifiFlowStatusResponse.ProcessorStatusEntry(processor)), List.of());
        var root = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        when(nifiClient.getRootFlowStatus())
                .thenReturn(new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(root)));

        List<NifiCountersResponse.Counter> counters = counterValue == null ? List.of()
                : List.of(new NifiCountersResponse.Counter("c1", "load-dz-POP003L (" + PROCESSOR_ID + ")",
                        counterName, counterValue));
        when(nifiClient.getCounters()).thenReturn(new NifiCountersResponse(
                new NifiCountersResponse.Counters(new NifiCountersResponse.AggregateSnapshot(counters))));
    }

    private NifiProcessorRun savedRun() {
        ArgumentCaptor<NifiProcessorRun> captor = ArgumentCaptor.forClass(NifiProcessorRun.class);
        verify(runRepository, atLeastOnce()).save(captor.capture());
        return captor.getValue();
    }

    @Test
    void activeThreadWithoutAnyRowsYet_opensRun() {
        // 추출이 오래 걸려 아직 한 행도 안 넣은 구간도 실행에 포함돼야 한다.
        // 카운터만 보면 이 구간이 빠져서 소요시간이 실제보다 짧게 나온다.
        nifiReports(1, null);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());

        tracker().track();

        NifiProcessorRun run = savedRun();
        assertThat(run.getProcessorId()).isEqualTo(PROCESSOR_ID);
        assertThat(run.getGroupName()).isEqualTo("DZ");
        assertThat(run.getTargetTable()).isEqualTo("public.dz_pop003l");
        assertThat(run.getStatus()).isEqualTo(NifiProcessorRun.STATUS_RUNNING);
        assertThat(run.getEndedAt()).isNull();
        assertThat(run.getInsertedCount()).isZero();
    }

    @Test
    void briefIdleShorterThanThreshold_doesNotCloseRun() {
        // 이게 핵심이다. 한 주기 조용하다고 닫아버리면 한 번의 적재가 여러 조각으로
        // 쪼개져서, 고치려던 문제(6분 적재가 7행)로 그대로 되돌아간다.
        nifiReports(0, null);
        NifiProcessorRun open = new NifiProcessorRun(PROCESSOR_ID, "load-dz-POP003L", "PutDatabaseRecord",
                GROUP_ID, "DZ", "public.dz_pop003l", at().minusMinutes(3));
        open.observeActivity(at().minusSeconds(30), 1_000L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(open));

        tracker().track();

        verify(runRepository, never()).save(any());
        assertThat(open.getEndedAt()).isNull();
    }

    @Test
    void idleLongerThanThreshold_closesRunAtLastActivityNotNow() {
        // 종료 시각을 "닫은 시각"으로 잡으면 유휴 확인에 쓴 45초가 소요시간에 얹혀
        // 처리량이 실제보다 낮게 나온다. 마지막으로 활동을 본 시각이어야 한다.
        nifiReports(0, null);
        LocalDateTime lastActivity = at().minusSeconds(60);
        NifiProcessorRun open = new NifiProcessorRun(PROCESSOR_ID, "load-dz-POP003L", "PutDatabaseRecord",
                GROUP_ID, "DZ", "public.dz_pop003l", at().minusMinutes(5));
        open.observeActivity(lastActivity, 5_000L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(open));

        tracker().track();

        NifiProcessorRun closed = savedRun();
        assertThat(closed.getStatus()).isEqualTo(NifiProcessorRun.STATUS_SUCCESS);
        assertThat(closed.getEndedAt()).isEqualTo(lastActivity);
        assertThat(closed.getInsertedCount()).isEqualTo(5_000L);
    }

    @Test
    void counterDelta_accumulatesIntoTheSameRunAcrossCycles() {
        NifiProcessorRunTracker tracker = tracker();

        // 1주기: 처음 본 카운터는 기준점만 잡는다(그 이전 누적을 이번 실행분으로 세면 안 됨).
        nifiReports(1, 1_000L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());
        tracker.track();
        NifiProcessorRun run = savedRun();
        assertThat(run.getInsertedCount()).isZero();

        // 2주기: 1,000 -> 3,500 이면 증가분 2,500만 이 구간에 더한다.
        nifiReports(1, 3_500L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));
        tracker.track();

        assertThat(run.getInsertedCount()).isEqualTo(2_500L);
        assertThat(run.getEndedAt()).isNull();
    }

    @Test
    void counterAppearingForTheFirstTime_countsItsWholeValue() {
        // NiFi 카운터는 첫 적재 전까지 응답에 없고, PutDatabaseRecord는 세션 커밋 때
        // 한 번에 올려서 "없음 -> 1,660,590"으로 단번에 나타난다. 이 순간을 "처음 보는
        // 프로세서"로 취급하면 증가분이 0이 된다 - 12:56 실행에서 8개 적재가 전부 0건으로
        // 기록되고 5개는 구간조차 안 생겼던 원인이다.
        NifiProcessorRunTracker tracker = tracker();

        // 1주기: 아직 안 돌아서 카운터가 없다. 스레드는 잡고 있어(추출 중) 구간은 열린다.
        nifiReports(1, null);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());
        tracker.track();
        NifiProcessorRun run = savedRun();
        assertThat(run.getInsertedCount()).isZero();

        // 2주기: 커밋되면서 카운터가 1,660,590으로 처음 등장 -> 전량이 이번 구간의 적재다.
        nifiReports(0, 1_660_590L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));
        tracker.track();

        assertThat(run.getInsertedCount()).isEqualTo(1_660_590L);
    }

    @Test
    void fastLoadSeenOnlyByCounter_stillOpensARun() {
        // COM001M(43건)처럼 한 주기 안에 끝나면 activeThreadCount는 계속 0이라,
        // 카운터 증가만으로 활동을 인정하지 못하면 구간이 아예 안 생긴다.
        NifiProcessorRunTracker tracker = tracker();

        nifiReports(0, null);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());
        tracker.track();
        verify(runRepository, never()).save(any());

        nifiReports(0, 43L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());
        tracker.track();

        NifiProcessorRun run = savedRun();
        assertThat(run.getInsertedCount()).isEqualTo(43L);
        assertThat(run.getStatus()).isEqualTo(NifiProcessorRun.STATUS_RUNNING);
    }

    @Test
    void upsertCounter_isTreatedAsLoadCounter() {
        NifiProcessorRunTracker tracker = tracker();

        nifiReports(0, null);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());
        tracker.track();

        nifiReports(0, 200L, "UPSERT updates performed");
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());
        tracker.track();

        NifiProcessorRun run = savedRun();
        assertThat(run.getInsertedCount()).isEqualTo(200L);
    }

    @Test
    void counterAlreadyPopulatedAtStartup_isOnlyABaseline() {
        // 앱이 막 떴을 때 카운터에 이미 쌓여 있는 값은 과거 적재분이라 세면 안 된다.
        nifiReports(0, 9_999_999L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());

        tracker().track();

        verify(runRepository, never()).save(any());
    }

    @Test
    void nonLoadProcessorType_isIgnored() {
        var processor = new NifiFlowStatusResponse.ProcessorStatus("p1", "parse-csv-and-tag", "UpdateRecord", 3);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "g1", "unstructured-csv",
                List.of(new NifiFlowStatusResponse.ProcessorStatusEntry(processor)), List.of());
        var root = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        when(nifiClient.getRootFlowStatus())
                .thenReturn(new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(root)));
        when(nifiClient.getCounters()).thenReturn(new NifiCountersResponse(
                new NifiCountersResponse.Counters(new NifiCountersResponse.AggregateSnapshot(List.of()))));
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of());

        tracker().track();

        verify(runRepository, never()).save(any());
    }
}
