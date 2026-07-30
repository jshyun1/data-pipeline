package com.company.pipeline.connector;

import com.company.pipeline.connector.dto.ConnectorPluginInfo;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.KafkaConnectWorkerInfo;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Kafka Connect REST API(:8083)를 호출하는 클라이언트.
 * PUT .../config는 register-connector.sh와 동일하게 멱등적으로 등록/갱신에 쓰인다.
 */
@Component
@EnableConfigurationProperties(KafkaConnectProperties.class)
public class KafkaConnectClient {

    private final RestClient restClient;

    public KafkaConnectClient(KafkaConnectProperties properties) {
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .build();
    }

    /** 워커 생존 확인용(대시보드 인프라 구역). 커넥터 목록보다 가볍고 항상 응답한다. */
    public KafkaConnectWorkerInfo getWorkerInfo() {
        return execute(() -> restClient.get()
                .uri("/")
                .retrieve()
                .body(KafkaConnectWorkerInfo.class));
    }

    public List<String> listConnectors() {
        return execute(() -> restClient.get()
                .uri("/connectors")
                .retrieve()
                .body(new ParameterizedTypeReference<List<String>>() { }));
    }

    public ConnectorStatusResponse getStatus(String connectorName) {
        return execute(() -> restClient.get()
                .uri("/connectors/{name}/status", connectorName)
                .retrieve()
                .body(ConnectorStatusResponse.class));
    }

    public Map<String, Object> getConfig(String connectorName) {
        return execute(() -> restClient.get()
                .uri("/connectors/{name}/config", connectorName)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() { }));
    }

    /** PUT은 멱등적: 커넥터가 없으면 생성, 있으면 설정을 갱신한다. */
    public Map<String, Object> upsertConfig(String connectorName, Map<String, Object> config) {
        return execute(() -> restClient.put()
                .uri("/connectors/{name}/config", connectorName)
                .body(config)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() { }));
    }

    /**
     * Kafka 3.7(KIP-980)의 initial_state를 사용해 커넥터를 처음부터 STOPPED로 만든다.
     * PUT /config는 생성 즉시 실행될 수 있으므로 CDC 준비 단계에서는 사용하지 않는다.
     */
    public Map<String, Object> createStopped(String connectorName, Map<String, Object> config) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("name", connectorName);
        request.put("config", config);
        request.put("initial_state", "STOPPED");
        return execute(() -> restClient.post()
                .uri("/connectors")
                .body(request)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() { }));
    }

    public void pause(String connectorName) {
        execute(() -> restClient.put()
                .uri("/connectors/{name}/pause", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void resume(String connectorName) {
        execute(() -> restClient.put()
                .uri("/connectors/{name}/resume", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void stop(String connectorName) {
        execute(() -> restClient.put()
                .uri("/connectors/{name}/stop", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void delete(String connectorName) {
        execute(() -> restClient.delete()
                .uri("/connectors/{name}", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void restartTask(String connectorName, int taskId) {
        execute(() -> restClient.post()
                .uri("/connectors/{name}/tasks/{taskId}/restart", connectorName, taskId)
                .retrieve()
                .toBodilessEntity());
    }

    public List<ConnectorPluginInfo> listPlugins() {
        return execute(() -> restClient.get()
                .uri("/connector-plugins")
                .retrieve()
                .body(new ParameterizedTypeReference<List<ConnectorPluginInfo>>() { }));
    }

    private <T> T execute(java.util.function.Supplier<T> call) {
        try {
            return call.get();
        } catch (RestClientException ex) {
            throw new KafkaConnectClientException("Kafka Connect 호출 실패: " + ex.getMessage(), ex);
        }
    }
}
