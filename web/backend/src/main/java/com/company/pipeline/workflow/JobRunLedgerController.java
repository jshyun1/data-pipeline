package com.company.pipeline.workflow;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.jobcatalog.EtlJobRun;
import java.time.LocalDateTime;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 잡 실행 원장 API.
 *
 * <p>호출자가 사람이 아니라 <b>Airflow와 NiFi</b>다. Airflow는 {@code _pipeline_svc_auth}가
 * {@code X-Service-Token}을 자동으로 얹고, NiFi는 플랜 B-1에서 InvokeHTTP에 같은 헤더를
 * 넣게 된다. 인가 강제가 켜져도 이 경로가 살아야 워크플로우가 돈다.
 *
 * <p>{@code by-pg} 콜백은 범위 A에서는 아무도 호출하지 않는 <b>예약된 자리</b>다. NiFi 잡
 * 종단에 InvokeHTTP 2개를 붙이는 순간(플랜 B-1) 완료 판정이 추정에서 확정으로 바뀌는데,
 * 그때 백엔드·Airflow 코드는 고칠 필요가 없다.
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
        ledgerService.completeByPg(nifiPgId, request == null ? null : request.rows(), false, null);
        return ApiResponse.success(null);
    }

    /** NiFi 실패 콜백(플랜 B-1). 이걸 받으면 Airflow sensor가 태스크를 즉시 실패시킨다. */
    @PostMapping("/by-pg/{nifiPgId}/fail")
    public ApiResponse<Void> failByPg(@PathVariable String nifiPgId,
                                      @RequestBody(required = false) FailRequest request) {
        ledgerService.completeByPg(nifiPgId, null, true,
                request == null ? "NiFi가 실패를 보고했습니다." : request.error());
        return ApiResponse.success(null);
    }

    public record OpenRequest(Long jobId, String nifiPgId, String workflowKey, String nodeKey,
                              String dagRunId, String taskId) {
    }

    public record ObserveRequest(Long rows) {
    }

    public record CompleteRequest(Long rows) {
    }

    public record FailRequest(String error) {
    }

    /** {@code completionSource}가 화면에서 "확정 / 추정"을 가르는 근거다. */
    public record RunView(Long id, Long jobId, String runToken, String status,
                          String triggerSource, String completionSource, Long rowsProcessed,
                          String errorMessage, LocalDateTime startedAt, LocalDateTime endedAt) {

        static RunView from(EtlJobRun run) {
            return new RunView(run.getId(), run.getJobId(), run.getRunToken(), run.getStatus(),
                    run.getTriggerSource(), run.getCompletionSource(), run.getRowsProcessed(),
                    run.getErrorMessage(), run.getStartedAt(), run.getEndedAt());
        }
    }
}
