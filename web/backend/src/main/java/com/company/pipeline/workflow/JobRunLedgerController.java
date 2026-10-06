package com.company.pipeline.workflow;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.jobcatalog.EtlJobRun;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 잡 실행 원장 API.
 *
 * <p>호출자가 사람이 아니라 <b>Airflow와 NiFi</b>다. Airflow는 {@code _pipeline_svc_auth}가
 * {@code X-Service-Token}을 자동으로 얹고, NiFi 쪽은 Cerebro NAR 의 «job 완료 감시»(JobCompletionReportingTask)가
 * 같은 헤더로 {@code by-pg} 콜백을 부른다(2026-10-05). 인가 강제가 켜져도 이 경로가 살아야 워크플로우가 돈다.
 */
@RestController
@RequestMapping("/api/etl/job-runs")
@RequirePermission(system = SystemCode.NIFI, bits = AccessBits.WRITE)
public class JobRunLedgerController {

    private final JobRunLedgerService ledgerService;

    public JobRunLedgerController(JobRunLedgerService ledgerService) {
        this.ledgerService = ledgerService;
    }

    /** Airflow가 job TaskGroup 진입 시 호출. 열려 있으면 입양한다. */
    @PostMapping
    public ApiResponse<RunView> open(@RequestBody OpenRequest request) {
        return ApiResponse.success(RunView.from(ledgerService.openOrAdopt(
                request.jobId(), request.nifiPgId(), request.workflowKey(),
                request.nodeKey(), request.dagRunId(), request.taskId())));
    }

    /**
     * NiFi job 완료 감시(Cerebro NAR · JobCompletionReportingTask)가 감시할 그룹 목록 - 지워지지 않은 ETL job 의 NiFi 그룹 id.
     * 감시는 이 목록에 있는 그룹만 보고한다(상위 폴더 · 템플릿 그룹이 «완료»로 보고되지 않게).
     */
    @GetMapping("/watched-groups")
    @RequirePermission(system = SystemCode.NIFI)
    public ApiResponse<List<String>> watchedGroups() {
        return ApiResponse.success(ledgerService.watchedGroupIds());
    }

    /**
     * 워크플로우 실행 한 번(Airflow dag run id)의 job 실행들 - 로그 화면이 실행 중인 job 의 «지금까지 적재 건수»를 읽는다(2026-10-06).
     * 끝난 job 의 건수는 지금처럼 Airflow 작업 로그에서 읽는다.
     */
    @GetMapping("/by-dag-run")
    @RequirePermission(system = SystemCode.NIFI)
    public ApiResponse<List<RunView>> byDagRun(@RequestParam String dagRunId) {
        return ApiResponse.success(ledgerService.runsOfDagRun(dagRunId).stream().map(RunView::from).toList());
    }

    /** Airflow sensor가 완료를 확인하려고 폴링한다. */
    @GetMapping("/{runToken}")
    @RequirePermission(system = SystemCode.NIFI)
    public ApiResponse<RunView> get(@PathVariable String runToken) {
        return ApiResponse.success(RunView.from(ledgerService.byToken(runToken)));
    }

    /** 유휴 관측으로 끝났음을 기록(추정). 콜백이 이미 닫았으면 덮어쓰지 않는다. */
    @PostMapping("/{runToken}/observe-complete")
    public ApiResponse<Void> observeComplete(@PathVariable String runToken,
                                             @RequestBody(required = false) ObserveRequest request) {
        ledgerService.completeObserved(runToken, request == null ? null : request.rows());
        return ApiResponse.success(null);
    }

    /** Airflow가 실패로 판정했음을 원장에 남긴다(추정). 없으면 유휴 정리가 성공으로 닫는다. */
    @PostMapping("/{runToken}/observe-failed")
    public ApiResponse<Void> observeFailed(@PathVariable String runToken,
                                            @RequestBody(required = false) FailRequest request) {
        ledgerService.failObserved(runToken, request == null ? null : request.error());
        return ApiResponse.success(null);
    }

    /** NiFi 완료 콜백(플랜 B-1). 재전송돼도 안전하다(이미 닫혔으면 no-op). */
    @PostMapping("/by-pg/{nifiPgId}/complete")
    public ApiResponse<Void> completeByPg(@PathVariable String nifiPgId,
                                          @RequestBody(required = false) CompleteRequest request) {
        ledgerService.completeByPg(nifiPgId, request == null ? null : request.rows(), false, null,
                request == null ? null : request.startedAt(), request == null ? null : request.endedAt());
        return ApiResponse.success(null);
    }

    /** NiFi 완료 감시의 진행 알림(2026-10-06) - 실행 중 «지금까지 적재 건수». 열린 실행이 없으면 아무것도 하지 않는다. */
    @PostMapping("/by-pg/{nifiPgId}/progress")
    public ApiResponse<Void> progressByPg(@PathVariable String nifiPgId, @RequestBody ProgressRequest request) {
        if (request != null && request.rows() != null && request.rows() >= 0) {
            ledgerService.progressByPg(nifiPgId, request.rows());
        }
        return ApiResponse.success(null);
    }

    /** NiFi 실패 콜백(플랜 B-1). 이걸 받으면 Airflow sensor가 태스크를 즉시 실패시킨다. */
    @PostMapping("/by-pg/{nifiPgId}/fail")
    public ApiResponse<Void> failByPg(@PathVariable String nifiPgId,
                                      @RequestBody(required = false) FailRequest request) {
        ledgerService.completeByPg(nifiPgId, null, true,
                request == null || request.error() == null ? "NiFi가 실패를 보고했습니다." : request.error(),
                request == null ? null : request.startedAt(), request == null ? null : request.endedAt());
        return ApiResponse.success(null);
    }

    public record OpenRequest(Long jobId, String nifiPgId, String workflowKey, String nodeKey,
                              String dagRunId, String taskId) {
    }

    public record ObserveRequest(Long rows) {
    }

    /** @param rows 이번 실행에서 지금까지 적재한 건수 */
    public record ProgressRequest(Long rows) {
    }

    /**
     * @param startedAt · endedAt NiFi 완료 감시가 본 실제 실행 시각(epoch ms, 선택). 있으면 원장의 시작 · 끝을 이것으로 맞춘다.
     */
    public record CompleteRequest(Long rows, Long startedAt, Long endedAt) {
    }

    public record FailRequest(String error, Long startedAt, Long endedAt) {

        public FailRequest(String error) {
            this(error, null, null);
        }
    }

    /**
     * {@code completionSource}가 화면에서 "확정 / 추정"을 가르는 근거다. {@code nodeKey} 는 로그 화면이 job 줄과 짝짓는 데 쓴다.
     * 열린 실행(endedAt 없음)의 {@code rowsProcessed} 는 «지금까지 적재 건수»다(NiFi 완료 감시의 진행 알림).
     */
    public record RunView(Long id, Long jobId, String runToken, String status,
                          String triggerSource, String completionSource, Long rowsProcessed,
                          String errorMessage, LocalDateTime startedAt, LocalDateTime endedAt, String nodeKey) {

        static RunView from(EtlJobRun run) {
            return new RunView(run.getId(), run.getJobId(), run.getRunToken(), run.getStatus(),
                    run.getTriggerSource(), run.getCompletionSource(), run.getRowsProcessed(),
                    run.getErrorMessage(), run.getStartedAt(), run.getEndedAt(), run.getNodeKey());
        }
    }
}
