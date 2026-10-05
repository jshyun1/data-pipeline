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
}
