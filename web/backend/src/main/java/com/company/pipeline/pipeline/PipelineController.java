package com.company.pipeline.pipeline;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.logpipeline.dto.LogPipelineCreateRequest;
import com.company.pipeline.monitoring.PipelineMetricSnapshotService;
import com.company.pipeline.monitoring.dto.PipelineMetricSnapshotResponse;
import com.company.pipeline.pipeline.dto.PipelineCommandHistoryResponse;
import com.company.pipeline.pipeline.dto.PipelineCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import com.company.pipeline.pipeline.dto.PipelineRuntimeStatusResponse;
import com.company.pipeline.pipeline.dto.PipelineConsistencyCheckResponse;
import com.company.pipeline.pipeline.dto.PipelineBatchCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineBatchCreateResponse;
import com.company.pipeline.pipeline.dto.StuckSinkResponse;
import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.security.RequestActorResolver;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/pipelines")
@RequirePermission(system = SystemCode.KAFKA)
public class PipelineController {

    private final PipelineService pipelineService;
    private final PipelineDeployService pipelineDeployService;
    private final PipelineCommandHistoryRepository pipelineCommandHistoryRepository;
    private final PipelineCommandHistoryRecorder pipelineCommandHistoryRecorder;
    private final PipelineMetricSnapshotService pipelineMetricSnapshotService;
    private final PipelineRuntimeStatusService pipelineRuntimeStatusService;
    private final PipelineConsistencyService pipelineConsistencyService;
    private final PipelineBatchCreateService pipelineBatchCreateService;
    private final SinkStuckRecordService sinkStuckRecordService;
    private final RequestActorResolver actorResolver;

    public PipelineController(PipelineService pipelineService, PipelineDeployService pipelineDeployService,
            PipelineCommandHistoryRepository pipelineCommandHistoryRepository,
            PipelineCommandHistoryRecorder pipelineCommandHistoryRecorder,
            PipelineMetricSnapshotService pipelineMetricSnapshotService,
            PipelineRuntimeStatusService pipelineRuntimeStatusService,
            PipelineConsistencyService pipelineConsistencyService,
            PipelineBatchCreateService pipelineBatchCreateService,
            SinkStuckRecordService sinkStuckRecordService,
            RequestActorResolver actorResolver) {
        this.pipelineService = pipelineService;
        this.pipelineDeployService = pipelineDeployService;
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
        this.pipelineCommandHistoryRecorder = pipelineCommandHistoryRecorder;
        this.pipelineMetricSnapshotService = pipelineMetricSnapshotService;
        this.pipelineRuntimeStatusService = pipelineRuntimeStatusService;
        this.pipelineConsistencyService = pipelineConsistencyService;
        this.pipelineBatchCreateService = pipelineBatchCreateService;
        this.sinkStuckRecordService = sinkStuckRecordService;
        this.actorResolver = actorResolver;
    }

    // 등록 세 곳은 등록자를 반드시 남긴다 - 모르면 401(다시 로그인). 예전에는 늘 null 이라 AI 가 «확인되지 않음»이라 답했다(2026-10-13).
    @PostMapping
    public ApiResponse<PipelineResponse> create(@Valid @RequestBody PipelineCreateRequest request) {
        PipelineResponse created = pipelineService.create(request, actorResolver.requireForRegistration("CDC 파이프라인"));
        return ApiResponse.success(pipelineDeployService.deploy(created.id()));
    }

    @PostMapping("/batch")
    public ApiResponse<PipelineBatchCreateResponse> createBatch(
            @Valid @RequestBody PipelineBatchCreateRequest request) {
        return ApiResponse.success(pipelineBatchCreateService.create(request, actorResolver.requireForRegistration("CDC 파이프라인")));
    }

    @PostMapping("/log-file")
    public ApiResponse<PipelineResponse> createLogFilePipeline(@Valid @RequestBody LogPipelineCreateRequest request) {
        return ApiResponse.success(pipelineService.createLogFilePipeline(request, actorResolver.requireForRegistration("로그 파이프라인")));
    }

    @GetMapping
    public ApiResponse<List<PipelineResponse>> list() {
        return ApiResponse.success(pipelineService.list());
    }

    @GetMapping("/runtime-statuses")
    public ApiResponse<List<PipelineRuntimeStatusResponse>> runtimeStatuses() {
        return ApiResponse.success(pipelineRuntimeStatusService.list());
    }

    @GetMapping("/{id}/runtime-status")
    public ApiResponse<PipelineRuntimeStatusResponse> runtimeStatus(@PathVariable Long id) {
        return ApiResponse.success(pipelineRuntimeStatusService.get(id));
    }

    @GetMapping("/{id}")
    public ApiResponse<PipelineResponse> get(@PathVariable Long id) {
        return ApiResponse.success(pipelineService.get(id));
    }

    /**
     * 파이프라인 제어 이력(화면 «이력» 탭).
     *
     * <p>기본은 <b>사람이 한 제어만</b> 최근 200건이다. 30초 주기 자동 감시({@code RUNTIME_MONITOR})가
     * 수천 건이라, 그대로 주면 시작 · 중지 · 배포 · <b>건너뛰기(SKIP_RECORD)</b> 같은 실제 조치가 묻힌다.
     * 전부 보려면 {@code includeAuto=true}.
     */
    @GetMapping("/{id}/history")
    public ApiResponse<List<PipelineCommandHistoryResponse>> history(
            @PathVariable Long id,
            @RequestParam(defaultValue = "false") boolean includeAuto,
            @RequestParam(defaultValue = "200") int limit) {
        int size = Math.max(1, Math.min(limit, 1000));
        return ApiResponse.success(pipelineCommandHistoryRepository
                .findRecent(id, includeAuto, org.springframework.data.domain.PageRequest.of(0, size))
                .stream().map(PipelineCommandHistoryResponse::from).toList());
    }

    @GetMapping("/{id}/consistency-checks")
    public ApiResponse<List<PipelineConsistencyCheckResponse>> consistencyChecks(@PathVariable Long id) {
        return ApiResponse.success(pipelineConsistencyService.history(id));
    }

    @PostMapping("/{id}/consistency-checks")
    public ApiResponse<PipelineConsistencyCheckResponse> checkConsistency(@PathVariable Long id) {
        return ApiResponse.success(pipelineConsistencyService.check(id));
    }

    @PostMapping("/{id}/deploy")
    public ApiResponse<PipelineResponse> deploy(@PathVariable Long id) {
        return ApiResponse.success(pipelineDeployService.deploy(id));
    }

    @PostMapping("/{id}/start")
    public ApiResponse<PipelineResponse> start(@PathVariable Long id) {
        return ApiResponse.success(pipelineDeployService.resume(id));
    }

    @PostMapping("/{id}/pause")
    public ApiResponse<PipelineResponse> pause(@PathVariable Long id) {
        return ApiResponse.success(pipelineDeployService.pause(id));
    }

    @PostMapping("/{id}/stop")
    public ApiResponse<PipelineResponse> stop(@PathVariable Long id) {
        return ApiResponse.success(pipelineDeployService.stop(id));
    }

    /** 멈춘 싱크가 어디서 멈췄는지와 그 레코드(마스킹·길이 제한). 읽기만 한다. */
    @GetMapping("/{id}/sink-stuck")
    public ApiResponse<StuckSinkResponse> stuckSink(@PathVariable Long id) {
        return ApiResponse.success(sinkStuckRecordService.inspect(id));
    }

    /**
     * 고칠 수 없는 한 건을 건너뛰고 싱크를 다시 띄운다. <b>그 건은 타깃에 들어가지 않는다</b> - 사유가 필수이고 이력에 남는다.
     */
    @PostMapping("/{id}/sink-stuck/skip")
    public ApiResponse<PipelineResponse> skipStuckRecord(@PathVariable Long id,
            @Valid @RequestBody SkipStuckRecordRequest request, @AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(sinkStuckRecordService.skip(id, request.topic(), request.partition(),
                request.offset(), request.reason(), user == null ? "unknown" : user.getUserId()));
    }

    /** 화면에서 본 위치를 그대로 돌려받아, 그 사이 위치가 바뀌었으면 거절한다. */
    public record SkipStuckRecordRequest(
            @jakarta.validation.constraints.NotBlank String topic,
            @jakarta.validation.constraints.NotNull Integer partition,
            @jakarta.validation.constraints.NotNull Long offset,
            @jakarta.validation.constraints.NotBlank @jakarta.validation.constraints.Size(max = 500) String reason) {
    }

    /** 이미 등록된 싱크 커넥터에 지금 템플릿 설정(실패 정책 등)을 다시 넣는다. 소스는 건드리지 않는다. */
    @PostMapping("/{id}/sink-config/refresh")
    public ApiResponse<PipelineResponse> refreshSinkConfig(@PathVariable Long id) {
        return ApiResponse.success(pipelineDeployService.refreshSinkConfig(id));
    }

    @PostMapping("/{id}/restart")
    public ApiResponse<PipelineResponse> restart(@PathVariable Long id) {
        return ApiResponse.success(pipelineDeployService.restart(id));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        pipelineService.delete(id);
        return ApiResponse.success(null);
    }

    /**
     * 대시보드의 "커넥터 불일치" 경고를 닫는다 - 실제로 다시 배포하는 게 아니라,
     * 이 파이프라인은 확인했고 당장은 그대로 둬도 된다는 걸 기록만 남긴다
     * (예: 더 이상 안 쓰는 테스트 파이프라인이라 재배포가 필요 없는 경우).
     * 다음에 이 파이프라인에 새 배포/제어 명령이 들어오면 이 기록은 더 이상
     * 유효하지 않은 것으로 취급되어(DashboardController 참고) 경고가 다시 뜰 수 있다.
     */
    @PostMapping("/{id}/dismiss-drift")
    public ApiResponse<Void> dismissDrift(@PathVariable Long id) {
        pipelineService.get(id); // 존재하지 않는 파이프라인이면 404
        pipelineCommandHistoryRecorder.record(id, "DISMISS_DRIFT", "SUCCESS", null);
        return ApiResponse.success(null);
    }

    /**
     * 파이프라인의 싱크 커넥터가 지금까지 커밋한 offset을 스냅샷으로 남긴다. 대시보드용
     * "실제 적재 건수"를 위해 두 시점에 각각 호출해서 그 차이(delta)를 계산한다
     * (예전 CDC 제어 DAG 의 적재 검증이 쓰던 통로, 2026-10-01 제어 DAG 폐지 후에도 API 는 남겨 둔다).
     */
    @PostMapping("/{id}/metrics/snapshot")
    public ApiResponse<PipelineMetricSnapshotResponse> recordMetricSnapshot(@PathVariable Long id) {
        return ApiResponse.success(PipelineMetricSnapshotResponse.from(
                pipelineMetricSnapshotService.recordSnapshot(id)));
    }
}
