package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.dto.ConnectorPluginInfo;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 읽기 전용 Kafka Connect 상태 조회. docs/kafka-webservice-design.md §8.3 일부.
 * 커넥터 생성/배포(PipelineDeployService)는 다음 증분.
 */
@RestController
@RequestMapping("/api/connect")
public class MonitoringController {

    private final KafkaConnectClient kafkaConnectClient;

    public MonitoringController(KafkaConnectClient kafkaConnectClient) {
        this.kafkaConnectClient = kafkaConnectClient;
    }

    @GetMapping("/connectors")
    public ApiResponse<List<String>> listConnectors() {
        return ApiResponse.success(kafkaConnectClient.listConnectors());
    }

    @GetMapping("/connectors/{connectorName}/status")
    public ApiResponse<ConnectorStatusResponse> getStatus(@PathVariable String connectorName) {
        return ApiResponse.success(kafkaConnectClient.getStatus(connectorName));
    }

    @GetMapping("/connector-plugins")
    public ApiResponse<List<ConnectorPluginInfo>> listPlugins() {
        return ApiResponse.success(kafkaConnectClient.listPlugins());
    }
}
