package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.monitoring.dto.DashboardSummaryResponse;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
import com.company.pipeline.pipeline.dto.PipelineCommandHistoryResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 읽기 전용 대시보드 요약. docs/kafka-webservice-design.md §9.1/§8.3. */
@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineCommandHistoryRepository pipelineCommandHistoryRepository;
    private final KafkaConnectClient kafkaConnectClient;
    private final KafkaBrokerHealthChecker kafkaBrokerHealthChecker;

    public DashboardController(PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineCommandHistoryRepository pipelineCommandHistoryRepository,
            KafkaConnectClient kafkaConnectClient,
            KafkaBrokerHealthChecker kafkaBrokerHealthChecker) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
        this.kafkaConnectClient = kafkaConnectClient;
        this.kafkaBrokerHealthChecker = kafkaBrokerHealthChecker;
    }

    @GetMapping("/summary")
    public ApiResponse<DashboardSummaryResponse> summary() {
        List<PipelineDefinition> pipelines = pipelineDefinitionRepository.findAll();
        long running = pipelines.stream().filter(p -> p.getStatus() == PipelineStatus.DEPLOYED).count();
        long failed = pipelines.stream().filter(p -> p.getStatus() == PipelineStatus.FAILED).count();
        long paused = pipelines.stream().filter(p -> p.getStatus() == PipelineStatus.PAUSED).count();

        boolean kafkaConnectHealthy;
        try {
            kafkaConnectClient.listConnectors();
            kafkaConnectHealthy = true;
        } catch (BusinessException ex) {
            kafkaConnectHealthy = false;
        }

        List<PipelineCommandHistoryResponse> recentErrors = pipelineCommandHistoryRepository
                .findTop10ByResultOrderByRequestedAtDesc("FAILED")
                .stream().map(PipelineCommandHistoryResponse::from).toList();
        List<PipelineCommandHistoryResponse> recentDeployments = pipelineCommandHistoryRepository
                .findTop10ByCommandOrderByRequestedAtDesc("DEPLOY")
                .stream().map(PipelineCommandHistoryResponse::from).toList();

        boolean kafkaBrokerHealthy = kafkaBrokerHealthChecker.isHealthy();

        return ApiResponse.success(new DashboardSummaryResponse(
                pipelines.size(), running, failed, paused, kafkaConnectHealthy, kafkaBrokerHealthy,
                recentErrors, recentDeployments));
    }
}
