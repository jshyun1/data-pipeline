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

    private static final Logger log = LoggerFactory.getLogger(EtlJobRunService.class);

    private final EtlJobRunRepository runRepository;
    private final EtlJobRepository jobRepository;
    private final NifiExecutionLogEntryRepository executionLogRepository;

    public EtlJobRunService(EtlJobRunRepository runRepository,
                            EtlJobRepository jobRepository,
                            NifiExecutionLogEntryRepository executionLogRepository) {
        this.runRepository = runRepository;
        this.jobRepository = jobRepository;
        this.executionLogRepository = executionLogRepository;
    }

    /**
     * 이 잡의 열린 실행을 돌려주고, 없으면 새로 연다.
     *
     * @param newStepRun 이번 호출이 새 스텝 구간 때문인지(실행의 스텝 수를 셀 때 쓴다)
     */
    @Transactional
    public Optional<EtlJobRun> openOrAttach(Long jobId, LocalDateTime now, boolean newStepRun) {
        if (jobId == null) {
            return Optional.empty();
        }
        EtlJobRun run = runRepository.findByJobIdAndEndedAtIsNull(jobId)
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
     */
    @Transactional
    public int closeIdleRuns(LocalDateTime now, long idleSeconds) {
        List<EtlJobRun> open = runRepository.findByEndedAtIsNull();
        int closed = 0;
        for (EtlJobRun run : open) {
            if (run.getLastSeenAt().plusSeconds(idleSeconds).isAfter(now)) {
                continue;
            }
            int failures = executionLogRepository.countErrorsForJobBetween(
                    run.getJobId(), run.getStartedAt(), run.getLastSeenAt());
            run.close(failures);
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
