package com.company.pipeline.infra;

import com.company.pipeline.infra.dto.AirflowHealthResponse;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Airflow 스케줄러/DAG 프로세서/메타DB가 살아 있는지 보는 전용 클라이언트.
 *
 * <p>이 엔드포인트는 인증이 필요 없다(compose의 airflow-apiserver healthcheck도 토큰 없이
 * 같은 URL을 쓴다). 화면의 다른 Airflow 조회는 브라우저 세션 쿠키로 nginx를 거쳐 가지만,
 * 프로세스 생존 여부는 로그인 상태와 무관하게 서버가 직접 확인해야 하므로 백엔드에서 부른다.
 */
@Component
@EnableConfigurationProperties(AirflowProperties.class)
public class AirflowHealthClient {

    private static final Logger log = LoggerFactory.getLogger(AirflowHealthClient.class);

    private final RestClient restClient;

    public AirflowHealthClient(AirflowProperties properties) {
        // Airflow가 멈춰 있을 때 대시보드 응답이 같이 늘어지지 않도록 짧게 끊는다.
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(3));
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }

    /** 조회 실패(= api-server 다운/응답 없음)는 예외 대신 빈 값으로 알린다. */
    public Optional<AirflowHealthResponse> getHealth() {
        try {
            return Optional.ofNullable(restClient.get()
                    .uri("/api/v2/monitor/health")
                    .retrieve()
                    .body(AirflowHealthResponse.class));
        } catch (RestClientException ex) {
            log.debug("Airflow health 조회 실패: {}", ex.getMessage());
            return Optional.empty();
        }
    }
}
