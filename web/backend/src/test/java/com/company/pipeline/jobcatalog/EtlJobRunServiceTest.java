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
    void 콜백으로_방금_닫힌_실행이면_새로_열지_않고_꼬리_활동을_붙인다() {
        // NiFi 완료 감시가 조용해진 직후 닫고, 관측기는 15초 뒤에 마지막 적재분을 본다(2026-10-05 이중 행 방지).
        EtlJobRun closed = new EtlJobRun(7L, T0);
        closed.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 100L, null, T0.plusSeconds(60));
        when(runRepository.findByJobIdAndEndedAtIsNull(7L)).thenReturn(Optional.empty());
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(7L)).thenReturn(Optional.of(closed));

        EtlJobRun run = service.openOrAttach(7L, T0.plusSeconds(75), true).orElseThrow();

        assertThat(run).isSameAs(closed);
        org.mockito.Mockito.verify(runRepository, org.mockito.Mockito.never())
                .save(org.mockito.ArgumentMatchers.argThat(saved -> saved != closed));
    }

    @Test
    void 콜백으로_닫힌지_오래됐거나_관측으로_닫힌_실행이면_새로_연다() {
        EtlJobRun old = new EtlJobRun(7L, T0);
        old.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 100L, null, T0.plusSeconds(60));
        when(runRepository.findByJobIdAndEndedAtIsNull(7L)).thenReturn(Optional.empty());
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(7L)).thenReturn(Optional.of(old));

        EtlJobRun later = service.openOrAttach(7L, T0.plusSeconds(60 + EtlJobRunService.CALLBACK_TAIL_SECONDS + 1), true)
                .orElseThrow();
        assertThat(later).isNotSameAs(old);

        EtlJobRun observed = new EtlJobRun(8L, T0);
        observed.completeBy(EtlJobRun.SOURCE_OBSERVED, EtlJobRun.STATUS_SUCCESS, 5L, null, T0.plusSeconds(60));
        when(runRepository.findByJobIdAndEndedAtIsNull(8L)).thenReturn(Optional.empty());
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(8L)).thenReturn(Optional.of(observed));
        assertThat(service.openOrAttach(8L, T0.plusSeconds(65), true).orElseThrow()).isNotSameAs(observed);
    }

    @Test
    void 백엔드가_멈췄다_깨어나도_직전_관측_뒤에_닫힌_실행이면_꼬리를_붙인다() {
        // 시험 5: 관측기가 16:58:20 에 마지막으로 보고 멈췄다가 16:59:36 에 깨어나 그 사이 실행(16:58:25 콜백 마감)의 적재를 봤다.
        EtlJobRun closed = new EtlJobRun(7L, T0);
        closed.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 12L, null, T0.plusSeconds(25));
        when(runRepository.findByJobIdAndEndedAtIsNull(7L)).thenReturn(Optional.empty());
        when(runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(7L)).thenReturn(Optional.of(closed));

        EtlJobRun run = service.openOrAttach(7L, T0.plusSeconds(96), true, T0.plusSeconds(20)).orElseThrow();
        assertThat(run).isSameAs(closed);

        // 직전 관측이 마감보다 뒤였다면 지금 본 활동은 새 실행이다.
        assertThat(service.openOrAttach(7L, T0.plusSeconds(96), true, T0.plusSeconds(81)).orElseThrow()).isNotSameAs(closed);
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

    @Test
    void 워크플로우가_연_실행은_추출로_오래_조용해도_닫지_않는다() {
        // 추출이 1분 넘게 걸리는 동안 적재 프로세서가 조용했다 - 예전에는 45초 뒤 «성공 0건»으로 닫혀 완료 신호가 새 행으로 갔다(2026-10-13).
        EtlJobRun run = new EtlJobRun(7L, T0);
        run.adoptByWorkflow("token", "wf", "node", "manual__1", "node.open_run");
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));

        int closed = service.closeIdleRuns(T0.plusSeconds(120), 45);

        assertThat(closed).isZero();
        assertThat(run.getEndedAt()).isNull();
        assertThat(run.getStatus()).isEqualTo(EtlJobRun.STATUS_RUNNING);
    }

    @Test
    void 워크플로우가_연_실행을_아무도_마감하지_않으면_오래_뒤_실패로_정리하고_판정은_비워_둔다() {
        EtlJobRun run = new EtlJobRun(7L, T0);
        run.adoptByWorkflow("token", "wf", "node", "manual__1", "node.open_run");
        run.observeActivity(T0.plusSeconds(30), 10L);
        when(runRepository.findByEndedAtIsNull()).thenReturn(List.of(run));

        int closed = service.closeIdleRuns(T0.plusSeconds(30 + EtlJobRunService.WORKFLOW_ABANDON_SECONDS + 1), 45);

        assertThat(closed).isEqualTo(1);
        assertThat(run.getStatus()).isEqualTo(EtlJobRun.STATUS_FAILED);
        assertThat(run.getEndedAt()).isEqualTo(T0.plusSeconds(30));
        assertThat(run.getErrorMessage()).contains("3시간");
        assertThat(run.getCompletionSource()).isNull();
        // 늦게라도 실제 판정이 오면 덮어쓴다.
        run.completeBy(EtlJobRun.SOURCE_CALLBACK, EtlJobRun.STATUS_SUCCESS, 10L, null, T0.plusSeconds(40));
        assertThat(run.getStatus()).isEqualTo(EtlJobRun.STATUS_SUCCESS);
    }
}
