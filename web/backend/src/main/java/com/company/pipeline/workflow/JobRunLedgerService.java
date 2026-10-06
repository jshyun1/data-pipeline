package com.company.pipeline.workflow;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
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

    private final JdbcTemplate jdbc;

    public JobRunLedgerService(EtlJobRunRepository runRepository, EtlJobRepository jobRepository, JdbcTemplate jdbc) {
        this.runRepository = runRepository;
        this.jobRepository = jobRepository;
        this.jdbc = jdbc;
    }

    /**
     * 같은 job 의 원장 변경을 이 트랜잭션이 끝날 때까지 한 줄로 세운다(PostgreSQL advisory lock, 커밋 · 롤백 때 풀림).
     * 백엔드가 멈췄다 깨어나면 감시의 재전송 여러 개가 <b>동시에</b> 들어온다 - 서로의 저장을 못 본 채 모두 «처음»으로 판단해
     * 실행 1건이 5행이 됐다(2026-10-05 시험 5). 관측기(EtlJobRunService)도 같은 열쇠를 쓴다.
     */
    public static void lockJob(JdbcTemplate jdbc, Long jobId) {
        if (jdbc != null && jobId != null) {
            jdbc.query("SELECT pg_advisory_xact_lock(7301, ?)", rs -> null, jobId.intValue());
        }
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

    /** NiFi 완료 감시가 볼 그룹 - 지워지지 않은 ETL job 의 NiFi 그룹 id. */
    @Transactional(readOnly = true)
    public List<String> watchedGroupIds() {
        return jobRepository.findByDeletedAtIsNullOrderByJobNameAsc().stream()
                .map(EtlJob::getNifiPgId)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList();
    }

    /** NiFi 완료 콜백(플랜 B-1). 이미 닫힌 실행이면 조용히 넘어간다(재전송 대비). */
    @Transactional
    public void completeByPg(String nifiPgId, Long rows, boolean failed, String errorMessage) {
        completeByPg(nifiPgId, rows, failed, errorMessage, null, null);
    }

    /**
     * @param startedAtMillis · endedAtMillis NiFi 완료 감시가 본 실제 실행 시각(epoch ms). 없으면 받은 시각으로 닫는다.
     *        시작은 원장보다 이를 때만 당긴다 - 워크플로우가 연 실행은 NiFi 를 켜기 전에 열려 이미 더 이르다.
     *        관측기가 연 실행은 첫 관측(15초 간격)에 열려 실제보다 늦다(2026-10-05 시험 2 에서 확인).
     */
    @Transactional
    public void completeByPg(String nifiPgId, Long rows, boolean failed, String errorMessage,
                             Long startedAtMillis, Long endedAtMillis) {
        Long jobId = resolveJobId(null, nifiPgId);
        lockJob(jdbc, jobId);
        Optional<EtlJobRun> open = runRepository.findByJobIdAndEndedAtIsNull(jobId);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime reportedStart = toLocal(startedAtMillis, now);
        LocalDateTime reportedEnd = toLocal(endedAtMillis, now);
        LocalDateTime end = reportedEnd != null ? reportedEnd : now;
        if (reportedEnd != null && isResend(jobId, reportedEnd)) {
            // 같은 실행의 재전송이다(감시가 응답을 못 받아 다시 보냈는데, 백엔드는 앞의 것을 이미 처리했다 - 2026-10-05 시험 5 에서 6행이 생겼다).
            log.info("잡 실행 콜백 재전송 무시 - jobId={} endedAt={}", jobId, reportedEnd);
            return;
        }
        if (open.isEmpty()) {
            // Airflow 없이 NiFi에서 직접 돌린 실행도 이력에 남긴다.
            LocalDateTime start = reportedStart != null && !reportedStart.isAfter(end) ? reportedStart : end;
            EtlJobRun standalone = runRepository.save(new EtlJobRun(jobId, start));
            standalone.completeBy(EtlJobRun.SOURCE_CALLBACK,
                    failed ? EtlJobRun.STATUS_FAILED : EtlJobRun.STATUS_SUCCESS,
                    rows, errorMessage, end);
            runRepository.save(standalone);
            log.info("잡 실행 콜백(원장 없이 시작된 실행) - jobId={} failed={}", jobId, failed);
            return;
        }
        EtlJobRun run = open.get();
        if (reportedStart != null && reportedStart.isBefore(run.getStartedAt())) {
            run.startedEarlier(reportedStart);
        }
        if (end.isBefore(run.getStartedAt())) {
            end = now;
        }
        run.completeBy(EtlJobRun.SOURCE_CALLBACK,
                failed ? EtlJobRun.STATUS_FAILED : EtlJobRun.STATUS_SUCCESS,
                rows, errorMessage, end);
        runRepository.save(run);
        log.info("잡 실행 콜백 - jobId={} runId={} failed={} rows={}", jobId, run.getId(), failed, rows);
    }

    /**
     * NiFi 완료 감시가 실행 중에 보내는 «지금까지 적재 건수»(2026-10-06). 열린 실행에만 적고, 없거나 모르는 그룹이면 넘어간다 -
     * 진행 알림은 놓쳐도 되는 정보라 오류로 만들지 않는다(완료 알림이 최종 건수를 확정한다).
     * 2초마다 올 수 있어 job 은 전체 목록이 아니라 그룹 id 로 바로 찾는다.
     */
    @Transactional
    public void progressByPg(String nifiPgId, long rows) {
        Optional<EtlJob> job = jobRepository.findByNifiPgId(nifiPgId).filter(found -> found.getDeletedAt() == null);
        if (job.isEmpty()) {
            return;
        }
        lockJob(jdbc, job.get().getId());
        runRepository.findByJobIdAndEndedAtIsNull(job.get().getId()).ifPresent(run -> {
            run.reportProgress(rows, LocalDateTime.now());
            runRepository.save(run);
        });
    }

    /** 워크플로우 실행 한 번이 연 job 실행들(열린 것 · 닫힌 것 모두). 로그 화면이 진행 중 적재 건수를 읽는다. */
    @Transactional(readOnly = true)
    public List<EtlJobRun> runsOfDagRun(String dagRunId) {
        return runRepository.findByAirflowDagRunIdOrderByStartedAtAsc(dagRunId);
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

    /** 이 job 의 가장 최근 «콜백으로 닫힌» 실행이 같은 끝 시각이면 재전송이다. 감시는 같은 실행을 같은 본문(같은 시각)으로 다시 보낸다. */
    private boolean isResend(Long jobId, LocalDateTime reportedEnd) {
        return runRepository.findFirstByJobIdAndEndedAtIsNotNullOrderByEndedAtDesc(jobId)
                .filter(run -> EtlJobRun.SOURCE_CALLBACK.equals(run.getCompletionSource()))
                .filter(run -> Math.abs(java.time.Duration.between(run.getEndedAt(), reportedEnd).toMillis()) < 1_000L)
                .isPresent();
    }

    /** epoch ms → 서버 시각. 없거나 미래(시계 차이)면 null. */
    private static LocalDateTime toLocal(Long epochMillis, LocalDateTime now) {
        if (epochMillis == null || epochMillis <= 0) {
            return null;
        }
        LocalDateTime at = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(epochMillis), java.time.ZoneId.systemDefault());
        return at.isAfter(now) ? null : at;
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
