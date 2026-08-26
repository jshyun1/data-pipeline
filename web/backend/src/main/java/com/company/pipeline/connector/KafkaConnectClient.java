package com.company.pipeline.connector;

import com.company.pipeline.connector.dto.ConnectorPluginInfo;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.KafkaConnectWorkerInfo;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Kafka Connect REST API(:8083)를 호출하는 클라이언트.
 * PUT .../config는 register-connector.sh와 동일하게 멱등적으로 등록/갱신에 쓰인다.
 */
@Component
@EnableConfigurationProperties(KafkaConnectProperties.class)
public class KafkaConnectClient {

    /** 리밸런스를 유발하는 제어 호출의 read 타임아웃. 조회(5s)와 분리한다. */
    private static final Duration CONTROL_READ_TIMEOUT = Duration.ofSeconds(60);

    private final RestClient restClient;
    private final RestClient controlClient;

    public KafkaConnectClient(KafkaConnectProperties properties) {
        // 조회용 - 외부 클라이언트 타임아웃 규약(설계서 3-2): connect 2s / read 5s.
        // 그 규약의 목적은 "느린 외부 시스템이 관제·알림을 붙잡지 못하게" 하는 것이다
        // (3-2 스레드 풀 격리 / 원칙 B "외부 호출은 수집기 한 곳으로 몰고 거기에만 타임아웃").
        // 주기 수집·대시보드 조회는 짧게 끊는 게 맞다.
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory(Duration.ofSeconds(5)))
                .build();

        // 제어용 - 커넥터 등록/갱신·정지·재개는 Kafka Connect 클러스터의 «리밸런스»를 유발해
        // 응답까지 수십 초가 걸릴 수 있다. 여기에 read 5s 를 그대로 적용하면 "요청은 반영됐는데
        // 응답을 못 받아 실패로 기록"되고, 파이프라인이 FAILED 로 떨어져 start 가 막힌다
        // (2026-08-26 실장애: PUT /config 타임아웃 → 설정은 바뀌었는데 상태만 FAILED).
        // 같은 함정을 NiFi 쪽 영향분석도 이미 지적했다("read 5초 일괄 적용이면 마법사 생성이
        // 깨진다 - 조립 경로 예외를 요청하세요"). 제어 호출은 요청 스레드에서 돌아 관제 루프를
        // 막지 않으므로, 규약의 의도를 지키면서 길게 잡는다.
        this.controlClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory(CONTROL_READ_TIMEOUT))
                .build();
    }

    private static ClientHttpRequestFactory requestFactory(Duration readTimeout) {
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(readTimeout);
        return factory;
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
        return execute(() -> controlClient.put()
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
        return execute(() -> controlClient.post()
                .uri("/connectors")
                .body(request)
                .retrieve()
                .body(new ParameterizedTypeReference<Map<String, Object>>() { }));
    }

    public void pause(String connectorName) {
        execute(() -> controlClient.put()
                .uri("/connectors/{name}/pause", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void resume(String connectorName) {
        execute(() -> controlClient.put()
                .uri("/connectors/{name}/resume", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void stop(String connectorName) {
        execute(() -> controlClient.put()
                .uri("/connectors/{name}/stop", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void delete(String connectorName) {
        execute(() -> controlClient.delete()
                .uri("/connectors/{name}", connectorName)
                .retrieve()
                .toBodilessEntity());
    }

    public void restartTask(String connectorName, int taskId) {
        execute(() -> controlClient.post()
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
            // 타임아웃은 "실패"가 아니라 "결과를 모름"이다. 호출부가 실제 상태를 확인해
            // 판정할 수 있도록 따로 구분해서 던진다.
            if (isTimeout(ex)) {
                throw new KafkaConnectTimeoutException(
                        "Kafka Connect 응답 대기 시간 초과(요청은 반영됐을 수 있음): " + ex.getMessage(), ex);
            }
            throw new KafkaConnectClientException("Kafka Connect 호출 실패: " + ex.getMessage(), ex);
        }
    }

    /** read/connect 타임아웃인지. JDK HttpClient 는 HttpTimeoutException, 그 밖엔 SocketTimeoutException. */
    private static boolean isTimeout(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof java.net.http.HttpTimeoutException
                    || t instanceof java.net.SocketTimeoutException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
