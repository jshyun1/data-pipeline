package com.company.pipeline.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** NiFi 완료 감시 콜백(2026-10-05): 실제 시각 반영 · 재전송 무시. */
class JobRunLedgerServiceTest {

    private static final String PG = "0af11bd8-01a1-1000-d8dd-c1256bace8e3";
    private final EtlJobRunRepository runRepository = mock(EtlJobRunRepository.class);
    private final EtlJobRepository jobRepository = mock(EtlJobRepository.class);
    private final JobRunLedgerService service = new JobRunLedgerService(runRepository, jobRepository, null);

    @BeforeEach
    void setUp() {
        EtlJob job = mock(EtlJob.class);
        when(job.getId()).thenReturn(34L);
        when(job.getNifiPgId()).thenReturn(PG);
        when(jobRepository.findAll()).thenReturn(List.of(job));
        when(runRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    private static long millis(LocalDateTime at) {
        return at.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    @Test
    void reportedStartPullsAnObserverOpenedRunEarlier_andEndUsesReportedTime() {
        LocalDateTime start = LocalDateTime.now().minusSeconds(60).withNano(0);
        EtlJobRun open = new EtlJobRun(34L, start.plusSeconds(13));          // 관측기가 13초 늦게 열었다
        when(runRepository.findByJobIdAndEndedAtIsNull(34L)).thenReturn(Optional.of(open));
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(34L)).thenReturn(Optional.empty());

        service.completeByPg(PG, 30L, false, null, millis(start), millis(start.plusSeconds(20)));

        assertThat(open.getStartedAt()).isEqualTo(start);
        assertThat(open.getEndedAt()).isEqualTo(start.plusSeconds(20));
        assertThat(open.getRowsProcessed()).isEqualTo(30L);
        assertThat(open.getCompletionSource()).isEqualTo(EtlJobRun.SOURCE_CALLBACK);
    }

    @Test
    void resendOfTheSameRun_isIgnored() {
        LocalDateTime end = LocalDateTime.now().minusSeconds(30).withNano(0);
        EtlJobRun closed = new EtlJobRun(34L, end.minusSeconds(2));
        closed.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 12L, null, end);
        when(runRepository.findByJobIdAndEndedAtIsNull(34L)).thenReturn(Optional.empty());
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(34L)).thenReturn(Optional.of(closed));

        service.completeByPg(PG, 12L, false, null, millis(end.minusSeconds(2)), millis(end));

        verify(runRepository, never()).save(any());
    }

    @Test
    void nextRunAfterAClosedOne_isRecorded() {
        LocalDateTime end = LocalDateTime.now().minusSeconds(90).withNano(0);
        EtlJobRun closed = new EtlJobRun(34L, end.minusSeconds(2));
        closed.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 12L, null, end);
        when(runRepository.findByJobIdAndEndedAtIsNull(34L)).thenReturn(Optional.empty());
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(34L)).thenReturn(Optional.of(closed));

        service.completeByPg(PG, 12L, false, null, millis(end.plusSeconds(28)), millis(end.plusSeconds(30)));

        verify(runRepository, org.mockito.Mockito.atLeastOnce()).save(any());
    }

    @Test
    void progressIsWrittenToTheOpenRunOnly() {
        // 2026-10-06: NiFi 완료 감시가 실행 중 «지금까지 적재 건수»를 보낸다 - 로그 화면이 실시간으로 읽는다.
        EtlJob job = mock(EtlJob.class);
        when(job.getId()).thenReturn(34L);
        when(jobRepository.findByNifiPgId(PG)).thenReturn(Optional.of(job));
        EtlJobRun open = new EtlJobRun(34L, LocalDateTime.now().minusSeconds(10));
        when(runRepository.findByJobIdAndEndedAtIsNull(34L)).thenReturn(Optional.of(open));

        service.progressByPg(PG, 12_000L);
        assertThat(open.getRowsProcessed()).isEqualTo(12_000L);

        open.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 30_000L, null, LocalDateTime.now());
        open.reportProgress(31_000L, LocalDateTime.now());   // 늦게 온 진행 알림은 닫힌 실행을 바꾸지 않는다
        assertThat(open.getRowsProcessed()).isEqualTo(30_000L);
    }

    @Test
    void progressForAnUnknownGroupOrWithoutAnOpenRun_isIgnored() {
        when(jobRepository.findByNifiPgId("unknown")).thenReturn(Optional.empty());
        service.progressByPg("unknown", 5L);

        EtlJob job = mock(EtlJob.class);
        when(job.getId()).thenReturn(34L);
        when(jobRepository.findByNifiPgId(PG)).thenReturn(Optional.of(job));
        when(runRepository.findByJobIdAndEndedAtIsNull(34L)).thenReturn(Optional.empty());
        service.progressByPg(PG, 5L);

        verify(runRepository, never()).save(any());
    }
}
