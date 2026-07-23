package com.company.pipeline.pipeline;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.logpipeline.dto.LogPipelineCreateRequest;
import com.company.pipeline.monitoring.PipelineMetricSnapshotService;
import com.company.pipeline.monitoring.dto.PipelineMetricSnapshotResponse;
import com.company.pipeline.pipeline.dto.PipelineCommandHistoryResponse;
import com.company.pipeline.pipeline.dto.PipelineCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineResponse;
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
    private final PipelineMetricSnapshotService pipelineMetricSnapshotService;

    public PipelineController(PipelineService pipelineService, PipelineDeployService pipelineDeployService,
            PipelineCommandHistoryRepository pipelineCommandHistoryRepository,
            PipelineMetricSnapshotService pipelineMetricSnapshotService) {
        this.pipelineService = pipelineService;
        this.pipelineDeployService = pipelineDeployService;
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
        this.pipelineMetricSnapshotService = pipelineMetricSnapshotService;
    }

    @PostMapping
    public ApiResponse<PipelineResponse> create(@Valid @RequestBody PipelineCreateRequest request) {
        return ApiResponse.success(pipelineService.create(request));
    }

    @PostMapping("/log-file")
    public ApiResponse<PipelineResponse> createLogFilePipeline(@Valid @RequestBody LogPipelineCreateRequest request) {
        return ApiResponse.success(pipelineService.createLogFilePipeline(request));
    }

    @GetMapping
    public ApiResponse<List<PipelineResponse>> list() {
        return ApiResponse.success(pipelineService.list());
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
