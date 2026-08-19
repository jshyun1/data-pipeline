package com.company.pipeline.pipeline;

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
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/pipelines")
public class PipelineController {

    private final PipelineService pipelineService;
    private final PipelineDeployService pipelineDeployService;
    private final PipelineCommandHistoryRepository pipelineCommandHistoryRepository;
    private final PipelineCommandHistoryRecorder pipelineCommandHistoryRecorder;
    private final PipelineMetricSnapshotService pipelineMetricSnapshotService;
    private final PipelineRuntimeStatusService pipelineRuntimeStatusService;
    private final PipelineConsistencyService pipelineConsistencyService;
    private final PipelineBatchCreateService pipelineBatchCreateService;

    public PipelineController(PipelineService pipelineService, PipelineDeployService pipelineDeployService,
            PipelineCommandHistoryRepository pipelineCommandHistoryRepository,
            PipelineCommandHistoryRecorder pipelineCommandHistoryRecorder,
            PipelineMetricSnapshotService pipelineMetricSnapshotService,
            PipelineRuntimeStatusService pipelineRuntimeStatusService,
            PipelineConsistencyService pipelineConsistencyService,
            PipelineBatchCreateService pipelineBatchCreateService) {
        this.pipelineService = pipelineService;
        this.pipelineDeployService = pipelineDeployService;
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
        this.pipelineCommandHistoryRecorder = pipelineCommandHistoryRecorder;
        this.pipelineMetricSnapshotService = pipelineMetricSnapshotService;
        this.pipelineRuntimeStatusService = pipelineRuntimeStatusService;
        this.pipelineConsistencyService = pipelineConsistencyService;
        this.pipelineBatchCreateService = pipelineBatchCreateService;
    }

    @PostMapping
    public ApiResponse<PipelineResponse> create(@Valid @RequestBody PipelineCreateRequest request) {
        PipelineResponse created = pipelineService.create(request);
        return ApiResponse.success(pipelineDeployService.deploy(created.id()));
    }

    @PostMapping("/batch")
    public ApiResponse<PipelineBatchCreateResponse> createBatch(
            @Valid @RequestBody PipelineBatchCreateRequest request) {
        return ApiResponse.success(pipelineBatchCreateService.create(request));
    }

    @PostMapping("/log-file")
    public ApiResponse<PipelineResponse> createLogFilePipeline(@Valid @RequestBody LogPipelineCreateRequest request) {
        return ApiResponse.success(pipelineService.createLogFilePipeline(request));
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

    @GetMapping("/{id}/history")
    public ApiResponse<List<PipelineCommandHistoryResponse>> history(@PathVariable Long id) {
        return ApiResponse.success(pipelineCommandHistoryRepository.findByPipelineIdOrderByRequestedAtDesc(id)
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
     * "실제 적재 건수"를 위해 Airflow의 kafka_pipelines_dynamic.py가 두 시점에 각각
     * 호출해서 그 차이(delta)를 계산한다.
     */
    @PostMapping("/{id}/metrics/snapshot")
    public ApiResponse<PipelineMetricSnapshotResponse> recordMetricSnapshot(@PathVariable Long id) {
        return ApiResponse.success(PipelineMetricSnapshotResponse.from(
                pipelineMetricSnapshotService.recordSnapshot(id)));
    }
}
