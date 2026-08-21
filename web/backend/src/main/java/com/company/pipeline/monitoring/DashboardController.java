package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.monitoring.dto.ConnectorDriftEntry;
import com.company.pipeline.monitoring.dto.DashboardSummaryResponse;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
import com.company.pipeline.pipeline.dto.PipelineCommandHistoryResponse;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 읽기 전용 대시보드 요약. docs/kafka-webservice-design.md §9.1/§8.3. */
@RestController
@RequestMapping("/api/dashboard")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.COMMON)
public class DashboardController {

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineCommandHistoryRepository pipelineCommandHistoryRepository;
    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final KafkaConnectClient kafkaConnectClient;
    private final KafkaBrokerHealthChecker kafkaBrokerHealthChecker;

    public DashboardController(PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineCommandHistoryRepository pipelineCommandHistoryRepository,
            PipelineConnectorRepository pipelineConnectorRepository,
            KafkaConnectClient kafkaConnectClient,
            KafkaBrokerHealthChecker kafkaBrokerHealthChecker) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineCommandHistoryRepository = pipelineCommandHistoryRepository;
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.kafkaConnectClient = kafkaConnectClient;
        this.kafkaBrokerHealthChecker = kafkaBrokerHealthChecker;
    }

    @GetMapping("/summary")
    public ApiResponse<DashboardSummaryResponse> summary() {
        List<PipelineDefinition> pipelines = pipelineDefinitionRepository.findAll();
        long running = pipelines.stream().filter(p -> p.getStatus() == PipelineStatus.DEPLOYED).count();
        long failed = pipelines.stream().filter(p -> p.getStatus() == PipelineStatus.FAILED).count();
        long paused = pipelines.stream().filter(p -> p.getStatus() == PipelineStatus.PAUSED).count();

        List<String> liveConnectorNames = null;
        boolean kafkaConnectHealthy;
        try {
            liveConnectorNames = kafkaConnectClient.listConnectors();
            kafkaConnectHealthy = true;
        } catch (BusinessException ex) {
            kafkaConnectHealthy = false;
        }

        // metadata-db는 이 커넥터가 등록돼 있다고 알고 있는데(pipeline_connector row 존재),
        // 실제 Kafka Connect 레지스트리엔 없는 경우 - 컨테이너 재기동 등으로 Kafka Connect
        // 쪽 등록이 조용히 리셋된 상태를 사람이 데이터 유실을 겪기 전에 미리 알아채기 위함.
        // Kafka Connect 자체가 응답을 못 하는 중이면(kafkaConnectHealthy=false) 전부
        // "없다"고 오판할 수 있으니 그럴 땐 계산 자체를 건너뛴다.
        List<ConnectorDriftEntry> connectorDrift = List.of();
        if (kafkaConnectHealthy) {
            Set<String> liveSet = new HashSet<>(liveConnectorNames);
            Map<Long, String> pipelineNames = pipelines.stream()
                    .collect(Collectors.toMap(PipelineDefinition::getId, PipelineDefinition::getName));
            connectorDrift = pipelineConnectorRepository.findAll().stream()
                    .filter(c -> !liveSet.contains(c.getConnectorName()))
                    .filter(c -> !isDismissed(c.getPipelineId()))
                    .map(c -> new ConnectorDriftEntry(
                            c.getPipelineId(),
                            pipelineNames.getOrDefault(c.getPipelineId(), "(삭제된 파이프라인)"),
                            c.getConnectorName(),
                            c.getConnectorRole()))
                    .toList();
        }

        List<PipelineCommandHistoryResponse> recentErrors = pipelineCommandHistoryRepository
                .findTop10ByResultOrderByRequestedAtDesc("FAILED")
                .stream().map(PipelineCommandHistoryResponse::from).toList();
        List<PipelineCommandHistoryResponse> recentDeployments = pipelineCommandHistoryRepository
                .findTop10ByCommandOrderByRequestedAtDesc("DEPLOY")
                .stream().map(PipelineCommandHistoryResponse::from).toList();

        boolean kafkaBrokerHealthy = kafkaBrokerHealthChecker.isHealthy();

        var todayStart = LocalDate.now().atStartOfDay();
        long todayCommandTotalCount = pipelineCommandHistoryRepository.countByRequestedAtGreaterThanEqual(todayStart);
        long todayCommandSuccessCount =
                pipelineCommandHistoryRepository.countByResultAndRequestedAtGreaterThanEqual("SUCCESS", todayStart);
        long todayCommandFailedCount =
                pipelineCommandHistoryRepository.countByResultAndRequestedAtGreaterThanEqual("FAILED", todayStart);

        return ApiResponse.success(new DashboardSummaryResponse(
                pipelines.size(), running, failed, paused, kafkaConnectHealthy, kafkaBrokerHealthy,
                connectorDrift, recentErrors, recentDeployments,
                todayCommandTotalCount, todayCommandSuccessCount, todayCommandFailedCount));
    }

    /**
     * 이 파이프라인에 걸린 가장 최근 명령이 "불일치 경고 닫기"였다면 사용자가 이미
     * 확인했다는 뜻이므로 경고를 다시 띄우지 않는다. 그 이후 배포/제어 명령이 또
     * 들어오면(예: 재배포) 그게 가장 최근 명령이 되어 이 판단이 자동으로 무효화된다.
     */
    private boolean isDismissed(Long pipelineId) {
        return pipelineCommandHistoryRepository.findFirstByPipelineIdOrderByRequestedAtDesc(pipelineId)
                .map(h -> "DISMISS_DRIFT".equals(h.getCommand()))
                .orElse(false);
    }
}
