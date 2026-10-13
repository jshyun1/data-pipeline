package com.company.pipeline.jobcatalog;

import com.company.pipeline.monitoring.NifiExecutionLogEntryRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 잡 실행 1회({@link EtlJobRun})를 여닫는다.
 *
 * <p>호출자는 {@link com.company.pipeline.monitoring.NifiProcessorRunTracker}다. 프로세서
 * 구간이 열릴 때 이 서비스가 잡 실행을 열거나 이미 열린 것에 붙이고, 잡 소속 프로세서가
 * 모두 조용해지면 닫는다.
 *
 * <p>실패 판정은 실행 구간 안에 남은 ERROR 로그({@code nifi_execution_log.level='ERROR'})로
 * 한다. NiFi bulletin은 5분만 남지만 그건 이미 수집 스케줄러가 테이블로 옮겨 두므로,
 * 실행이 길어도 놓치지 않는다.
 */
@Service
public class EtlJobRunService {

    /**
     * 콜백으로 닫힌 실행에 관측기의 꼬리 활동을 붙여 주는 시간. 관측 주기(15초) 두 번 + 여유.
     * 이보다 늦게 잡힌 활동은 새 실행으로 본다(같은 job 을 곧바로 다시 돌린 경우).
     */
    static final long CALLBACK_TAIL_SECONDS = 45;

    /**
     * 워크플로우(Airflow)가 연 실행을 유휴 정리가 «버려진 실행»으로 닫기까지 기다리는 시간(2026-10-13).
     *
     * <p>워크플로우 실행은 NiFi 완료 감시(콜백)나 Airflow 완료 대기 · 정지 단계가 결과를 알고 닫는다. 유휴 정리가 45초 기준으로
     * 닫으면 추출이 45초 넘게 걸리는 job 에서 적재 전에 «성공 0건»으로 닫혀 버렸다. 그 뒤 적재 · 진행 건수 · 완료 신호가 관측기가
     * 새로 연 행으로 가서, 워크플로우는 신호를 못 받고 느린 예비 판정으로 42초 늦게 끝났고 화면 진행 건수도 비었다
     * (DZ_POP003L, 원장 223 · 224). 이 시간은 Airflow 가 도중에 죽어 아무도 닫지 않을 때의 안전장치라 완료 대기 기본 제한(2시간)보다 길다.
     */
    static final long WORKFLOW_ABANDON_SECONDS = 3 * 60 * 60;

    private static final Logger log = LoggerFactory.getLogger(EtlJobRunService.class);

    private final EtlJobRunRepository runRepository;
    private final EtlJobRepository jobRepository;
    private final NifiExecutionLogEntryRepository executionLogRepository;

    private org.springframework.jdbc.core.JdbcTemplate jdbc;

    public EtlJobRunService(EtlJobRunRepository runRepository,
                            EtlJobRepository jobRepository,
                            NifiExecutionLogEntryRepository executionLogRepository) {
        this.runRepository = runRepository;
        this.jobRepository = jobRepository;
        this.executionLogRepository = executionLogRepository;
    }

    /** 원장 job 단위 잠금(JobRunLedgerService#lockJob)에 쓴다. 테스트에서는 비워 둔다. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    void setJdbc(org.springframework.jdbc.core.JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 이 잡의 열린 실행을 돌려주고, 없으면 새로 연다.
     *
     * @param newStepRun 이번 호출이 새 스텝 구간 때문인지(실행의 스텝 수를 셀 때 쓴다)
     */
    @Transactional
    public Optional<EtlJobRun> openOrAttach(Long jobId, LocalDateTime now, boolean newStepRun) {
        return openOrAttach(jobId, now, newStepRun, null);
    }

    /**
     * @param previousTickAt 관측기가 직전에 본 시각. 콜백으로 닫힌 실행이 그 뒤에 끝났으면, 지금 본 활동은 그 실행의 꼬리다.
     *        «지금부터 45초»로만 보면 백엔드가 멈췄다 깨어난 경우(관측이 몇 분 비었다) 꼬리를 새 실행으로 잘못 연다(2026-10-05 시험 5).
     */
    @Transactional
    public Optional<EtlJobRun> openOrAttach(Long jobId, LocalDateTime now, boolean newStepRun, LocalDateTime previousTickAt) {
        if (jobId == null) {
            return Optional.empty();
        }
        com.company.pipeline.workflow.JobRunLedgerService.lockJob(jdbc, jobId);
        Optional<EtlJobRun> open = runRepository.findByJobIdAndEndedAtIsNull(jobId);
        if (open.isEmpty()) {
            // 콜백(NiFi 완료 감시)이 방금 닫은 실행이면 새로 열지 않고 거기에 붙인다(2026-10-05). 감시는 «조용해진 직후» 닫고
            // 관측기는 15초마다 카운터를 보므로, 같은 실행의 마지막 적재분이 닫힌 뒤에 관측기에 잡힌다 - 새로 열면 실행 1건이
            // «콜백 성공 + 유휴정리 성공» 두 행으로 갈라지고 적재 건수도 나뉜다.
            Optional<EtlJobRun> justClosed = runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(jobId)
                    .filter(run -> EtlJobRun.SOURCE_CALLBACK.equals(run.getCompletionSource()))
                    .filter(run -> !run.getEndedAt().isBefore(previousTickAt != null
                            ? previousTickAt.minusSeconds(5) : now.minusSeconds(CALLBACK_TAIL_SECONDS)));
            if (justClosed.isPresent()) {
                return justClosed;
            }
        }
        EtlJobRun run = open
                .orElseGet(() -> {
                    EtlJobRun created = runRepository.save(new EtlJobRun(jobId, now));
                    log.info("잡 실행 시작 관측 - jobId={} runId={}", jobId, created.getId());
                    return created;
                });
        if (newStepRun) {
            run.addStepRun();
            runRepository.save(run);
        }
        return Optional.of(run);
    }

    @Transactional
    public void recordActivity(Long jobRunId, LocalDateTime at, long insertedDelta) {
        if (jobRunId == null) {
            return;
        }
        runRepository.findById(jobRunId).ifPresent(run -> {
            run.observeActivity(at, insertedDelta);
            runRepository.save(run);
        });
    }

    /**
     * 마지막 활동 이후 {@code idleSeconds}가 지난 실행을 닫는다.
     *
     * <p>스텝 구간이 모두 닫혔는지는 따로 보지 않는다 - 스텝 쪽 유휴 판정과 같은 기준을
     * 쓰므로, 스텝이 아직 열려 있다면 이 실행도 방금 활동을 본 것이라 닫히지 않는다.
     *
     * <p>워크플로우가 연 실행은 {@link #WORKFLOW_ABANDON_SECONDS} 동안 조용해야 «버려진 실행»(실패)으로 닫는다.
     */
    @Transactional
    public int closeIdleRuns(LocalDateTime now, long idleSeconds) {
        List<EtlJobRun> open = runRepository.findByEndedAtIsNull();
        int closed = 0;
        for (EtlJobRun run : open) {
            boolean byWorkflow = EtlJobRun.TRIGGER_WORKFLOW.equals(run.getTriggerSource());
            long limit = byWorkflow ? Math.max(idleSeconds, WORKFLOW_ABANDON_SECONDS) : idleSeconds;
            if (run.getLastSeenAt().plusSeconds(limit).isAfter(now)) {
                continue;
            }
            if (byWorkflow) {
                run.abandon("워크플로우가 " + limit / 3600 + "시간 넘게 마감하지 않아 정리함(Airflow 중단 의심)");
            } else {
                int failures = executionLogRepository.countErrorsForJobBetween(
                        run.getJobId(), run.getStartedAt(), run.getLastSeenAt());
                run.close(failures);
            }
            runRepository.save(run);
            closed++;
            String jobName = jobRepository.findById(run.getJobId())
                    .map(EtlJob::getJobName).orElse(String.valueOf(run.getJobId()));
            log.info("잡 실행 종료 - {} {} 스텝 {}개 {}건 ({}~{})", jobName, run.getStatus(),
                    run.getStepRunCount(), run.getTotalInserted(), run.getStartedAt(), run.getEndedAt());
        }
        return closed;
    }
}
