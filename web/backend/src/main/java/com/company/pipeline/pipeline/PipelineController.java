package com.company.pipeline.pipeline;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.logpipeline.dto.LogPipelineCreateRequest;
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

    public PipelineController(PipelineService pipelineService, PipelineDeployService pipelineDeployService,
            PipelineCommandHistoryRepository pipelineCommandHistoryRepository) {
        this.pipelineService = pipelineService;
        this.pipelineDeployService = pipelineDeployService;
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
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
}
