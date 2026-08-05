package com.company.pipeline.jobcatalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.company.pipeline.monitoring.NifiExecutionLogEntryRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EtlJobRunServiceTest {

    private static final LocalDateTime T0 = LocalDateTime.of(2026, 8, 4, 10, 0, 0);

    @Mock
    private EtlJobRunRepository runRepository;
    @Mock
    private EtlJobRepository jobRepository;
    @Mock
    private NifiExecutionLogEntryRepository executionLogRepository;

    private EtlJobRunService service;

    @BeforeEach
    void setUp() {
        service = new EtlJobRunService(runRepository, jobRepository, executionLogRepository);
        when(runRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(jobRepository.findById(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    void 같은_잡의_두_번째_스텝은_새_실행을_만들지_않고_붙는다() {
        EtlJobRun existing = new EtlJobRun(7L, T0);
        when(runRepository.findByJobIdAndEndedAtIsNull(7L)).thenReturn(Optional.of(existing));

        EtlJobRun run = service.openOrAttach(7L, T0.plusSeconds(30), true).orElseThrow();

        assertThat(run).isSameAs(existing);
        assertThat(run.getStepRunCount()).isEqualTo(1);
    }

    @Test
    void 잡을_해석하지_못하면_실행을_열지_않는다() {
        assertThat(service.openOrAttach(null, T0, true)).isEmpty();
    }

    @Test
    void 유휴가_지속되면_실행을_닫고_적재건수를_합산한다() {
        EtlJobRun run = new EtlJobRun(7L, T0);
        run.observeActivity(T0.plusSeconds(10), 1_000L);
        run.observeActivity(T0.plusSeconds(20), 660_590L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));
        when(executionLogRepository.countErrorsForJobBetween(anyLong(), any(), any())).thenReturn(0);

        int closed = service.closeIdleRuns(T0.plusSeconds(120), 45);

        assertThat(closed).isEqualTo(1);
        assertThat(run.getStatus()).isEqualTo(EtlJobRun.STATUS_SUCCESS);
        assertThat(run.getTotalInserted()).isEqualTo(661_590L);
        assertThat(run.getEndedAt()).isEqualTo(T0.plusSeconds(20));
    }

    @Test
    void 실행_구간에_ERROR가_있으면_FAILED로_닫는다() {
        EtlJobRun run = new EtlJobRun(7L, T0);
        run.observeActivity(T0.plusSeconds(10), 5L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));
        when(executionLogRepository.countErrorsForJobBetween(anyLong(), any(), any())).thenReturn(2);

        service.closeIdleRuns(T0.plusSeconds(120), 45);

        assertThat(run.getStatus()).isEqualTo(EtlJobRun.STATUS_FAILED);
        assertThat(run.getFailedStepCount()).isEqualTo(2);
    }

    @Test
    void 아직_활동_중이면_닫지_않는다() {
        EtlJobRun run = new EtlJobRun(7L, T0);
        run.observeActivity(T0.plusSeconds(100), 1L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));

        int closed = service.closeIdleRuns(T0.plusSeconds(120), 45);

        assertThat(closed).isZero();
        assertThat(run.getEndedAt()).isNull();
        assertThat(run.getStatus()).isEqualTo(EtlJobRun.STATUS_RUNNING);
    }
}
