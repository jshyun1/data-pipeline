package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.pipeline.dto.PipelineBatchCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineBatchCreateResponse;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.springframework.stereotype.Service;

/** DB 트랜잭션 밖의 Kafka Connect 부작용을 보상 삭제로 원자적으로 보이게 만든다. */
@Service
public class PipelineBatchCreateService {
    private final PipelineService pipelineService;
    private final PipelineDeployService pipelineDeployService;

    public PipelineBatchCreateService(PipelineService pipelineService, PipelineDeployService pipelineDeployService) {
        this.pipelineService = pipelineService;
        this.pipelineDeployService = pipelineDeployService;
    }

    public PipelineBatchCreateResponse create(PipelineBatchCreateRequest request) {
        List<PipelineResponse> definitions = pipelineService.createBatchDefinitions(request.pipelines());
        List<PipelineResponse> prepared = new ArrayList<>();
        try {
            for (PipelineResponse definition : definitions) {
                prepared.add(pipelineDeployService.deploy(definition.id()));
            }
            return new PipelineBatchCreateResponse(prepared.size(), List.copyOf(prepared));
        } catch (Exception cause) {
            List<String> cleanupFailures = compensate(definitions);
            String message = "배치 생성 중 실패하여 생성된 파이프라인을 모두 정리했습니다: " + cause.getMessage();
            if (!cleanupFailures.isEmpty()) {
                message += " (수동 정리 필요: " + String.join(", ", cleanupFailures) + ")";
            }
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, message, cause);
        }
    }

    private List<String> compensate(List<PipelineResponse> definitions) {
        List<PipelineResponse> reversed = new ArrayList<>(definitions);
        Collections.reverse(reversed);
        List<String> failures = new ArrayList<>();
        for (PipelineResponse definition : reversed) {
            try {
                pipelineService.delete(definition.id());
            } catch (Exception cleanupError) {
                failures.add(definition.name() + "(" + cleanupError.getMessage() + ")");
            }
        }
        return failures;
    }
}
