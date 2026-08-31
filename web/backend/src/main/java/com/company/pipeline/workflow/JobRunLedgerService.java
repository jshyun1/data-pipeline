package com.company.pipeline.workflow;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 잡 실행 원장. Airflow가 열고, NiFi 콜백(플랜 B-1) 또는 관측 추정이 닫는다.
 *
 * <p>기존 관측기({@code NifiProcessorRunTracker} → {@code EtlJobRunService})와 <b>같은 행</b>을
 * 쓴다. 관측기가 먼저 열어둔 실행이 있으면 새로 만들지 않고 입양한다 - 그러지 않으면 한 번의
 * 실행이 두 행으로 갈라져서 "몇 번 돌았나"가 어긋난다.
 *
 * <p>완료 콜백을 {@code runToken}이 아니라 <b>프로세스 그룹 id</b>로도 받는 이유: NiFi는 실행마다
 * 바뀌는 토큰을 알 방법이 없지만 자기 그룹 id는 파라미터로 늘 알고 있다. "한 job에 열린 실행은
 * 하나"({@code uq_etl_job_run_open})라서 그룹 id만으로 대상이 유일하게 정해진다.
 */
@Service
public class JobRunLedgerService {

    private static final Logger log = LoggerFactory.getLogger(JobRunLedgerService.class);

    private final EtlJobRunRepository runRepository;
    private final EtlJobRepository jobRepository;

    public JobRunLedgerService(EtlJobRunRepository runRepository, EtlJobRepository jobRepository) {
        this.runRepository = runRepository;
        this.jobRepository = jobRepository;
    }

    /** 열려 있으면 입양하고 없으면 연다. 동시 생성 경합은 재조회로 흡수한다. */
    @Transactional
    public EtlJobRun openOrAdopt(Long jobId, String nifiPgId, String workflowKey, String nodeKey,
                                 String dagRunId, String taskId) {
        Long resolvedJobId = resolveJobId(jobId, nifiPgId);
        LocalDateTime now = LocalDateTime.now();
        String token = UUID.randomUUID().toString().replace("-", "");

        EtlJobRun run = runRepository.findByJobIdAndEndedAtIsNull(resolvedJobId)
                .orElseGet(() -> {
                    try {
                        return runRepository.saveAndFlush(new EtlJobRun(resolvedJobId, now));
                    } catch (DataIntegrityViolationException ex) {
                        // uq_etl_job_run_open 경합 - 관측기가 방금 열었다면 그것을 쓴다.
                        return runRepository.findByJobIdAndEndedAtIsNull(resolvedJobId)
                                .orElseThrow(() -> ex);
                    }
                });
        run.adoptByWorkflow(token, workflowKey, nodeKey, dagRunId, taskId);
        runRepository.save(run);
        log.info("잡 실행 원장 open/adopt - jobId={} runId={} workflow={} node={}",
                resolvedJobId, run.getId(), workflowKey, nodeKey);
        return run;
    }

    @Transactional(readOnly = true)
    public EtlJobRun byToken(String runToken) {
        return runRepository.findAll().stream()
                .filter(run -> runToken.equals(run.getRunToken()))
                .findFirst()
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "실행을 찾을 수 없습니다: " + runToken));
    }

    /** NiFi 완료 콜백(플랜 B-1). 이미 닫힌 실행이면 조용히 넘어간다(재전송 대비). */
    @Transactional
    public void completeByPg(String nifiPgId, Long rows, boolean failed, String errorMessage) {
        Long jobId = resolveJobId(null, nifiPgId);
        Optional<EtlJobRun> open = runRepository.findByJobIdAndEndedAtIsNull(jobId);
        LocalDateTime now = LocalDateTime.now();
        if (open.isEmpty()) {
            // Airflow 없이 NiFi에서 직접 돌린 실행도 이력에 남긴다.
            EtlJobRun standalone = runRepository.save(new EtlJobRun(jobId, now));
            standalone.completeBy(EtlJobRun.SOURCE_CALLBACK,
                    failed ? EtlJobRun.STATUS_FAILED : EtlJobRun.STATUS_SUCCESS,
                    rows, errorMessage, now);
            runRepository.save(standalone);
            log.info("잡 실행 콜백(원장 없이 시작된 실행) - jobId={} failed={}", jobId, failed);
            return;
        }
        EtlJobRun run = open.get();
        run.completeBy(EtlJobRun.SOURCE_CALLBACK,
                failed ? EtlJobRun.STATUS_FAILED : EtlJobRun.STATUS_SUCCESS,
                rows, errorMessage, now);
        runRepository.save(run);
        log.info("잡 실행 콜백 - jobId={} runId={} failed={} rows={}", jobId, run.getId(), failed, rows);
    }

    /** 유휴 관측으로 끝났음을 기록한다(추정). 콜백이 이미 닫았으면 덮어쓰지 않는다. */
    @Transactional
    public void completeObserved(String runToken, Long rows) {
        EtlJobRun run = byToken(runToken);
        run.completeBy(EtlJobRun.SOURCE_OBSERVED, EtlJobRun.STATUS_SUCCESS, rows, null,
                LocalDateTime.now());
        runRepository.save(run);
    }

    /**
     * 실패로 끝났음을 기록한다.
     *
     * <p>없으면 실패한 실행이 원장에 열린 채 남고, 유휴 정리가 나중에 SUCCESS로 닫는다.
     * 실제로 NiFi 추출이 DB 접속 실패로 끝난 실행이 원장에는 성공으로 남아 있었다
     * (2026-08-31). Airflow는 실패로 보는데 원장만 성공이면 이력을 믿을 수 없다.
     */
    @Transactional
    public void failObserved(String runToken, String errorMessage) {
        EtlJobRun run = byToken(runToken);
        run.completeBy(EtlJobRun.SOURCE_OBSERVED, EtlJobRun.STATUS_FAILED, null,
                errorMessage, LocalDateTime.now());
        runRepository.save(run);
        log.info("잡 실행 실패 기록 - runId={} {}", run.getId(), errorMessage);
    }

    private Long resolveJobId(Long jobId, String nifiPgId) {
        if (jobId != null) {
            return jobId;
        }
        if (nifiPgId == null || nifiPgId.isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "jobId 또는 nifiPgId 중 하나는 있어야 합니다.");
        }
        return jobRepository.findAll().stream()
                .filter(job -> nifiPgId.equals(job.getNifiPgId()) && job.getDeletedAt() == null)
                .map(EtlJob::getId)
                .findFirst()
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "프로세스 그룹에 해당하는 job이 없습니다: " + nifiPgId));
    }
}
