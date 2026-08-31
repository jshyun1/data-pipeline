package com.company.pipeline.workflow;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

/**
 * 게시된 spec을 Airflow Variable에 올린다(구현계획 §4.3).
 *
 * <p>DAG 팩토리가 백엔드 DB에 직접 붙지 않게 하려고 Variable을 전달 통로로 쓴다. 팩토리는
 * top-level에서 Variable만 읽으므로 파싱이 백엔드·NiFi 가용성에 묶이지 않는다.
 *
 * <p>인증은 {@code AirflowDagRunClient}와 같은 방식이다 - 브라우저용 nginx 프록시(per-user
 * 세션)가 아니라 airflow-apiserver에 직접 붙고, 서비스 admin 계정으로 받은 Bearer JWT를 쓴다.
 * uvicorn이 JDK 기본 HTTP/2를 거부해서 HTTP/1.1로 고정하는 것도 동일하다(실측).
 */
@Component
public class AirflowVariableClient {

    /** 게시된 워크플로우 key 목록. 팩토리가 이걸 먼저 읽는다. */
    public static final String INDEX_KEY = "etl_wf_index";
    /** 워크플로우별 spec. {@code etl_wf_spec__{workflowKey}} */
    public static final String SPEC_PREFIX = "etl_wf_spec__";

    private static final long TOKEN_TTL_MS = 20 * 60 * 1000L;

    private final RestClient api;
    private final RestClient tokenClient;
    private final String adminUsername;
    private final String adminPassword;

    private volatile String cachedToken;
    private volatile long tokenExpiresAt;

    public AirflowVariableClient(
            @Value("${airflow.base-url:http://airflow-apiserver:8080}") String baseUrl,
            @Value("${airflow.admin-username:}") String adminUsername,
            @Value("${airflow.admin-password:}") String adminPassword) {
        this.adminUsername = adminUsername;
        this.adminPassword = adminPassword;
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(10));
        this.api = RestClient.builder()
                .baseUrl(baseUrl + "/airflow/api/v2").requestFactory(factory).build();
        this.tokenClient = RestClient.builder()
                .baseUrl(baseUrl + "/airflow").requestFactory(factory).build();
    }

    public static String specKey(String workflowKey) {
        return SPEC_PREFIX + workflowKey;
    }

    /** 없으면 만들고 있으면 덮어쓴다. Airflow는 upsert가 없어서 POST 409면 PATCH로 넘어간다. */
    public void upsert(String key, String value) {
        try {
            withAuth(token -> api.post().uri("/variables")
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("key", key, "value", value))
                    .retrieve().toBodilessEntity());
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() != 409) {
                throw ex;
            }
            withAuth(token -> api.patch().uri("/variables/{key}", key)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("key", key, "value", value))
                    .retrieve().toBodilessEntity());
        }
    }

    /** 이미 없으면 성공으로 본다(게시 취소를 여러 번 눌러도 안전하게). */
    public void delete(String key) {
        try {
            withAuth(token -> api.delete().uri("/variables/{key}", key)
                    .header("Authorization", "Bearer " + token)
                    .retrieve().toBodilessEntity());
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() != 404) {
                throw ex;
            }
        }
    }

    private synchronized String bearerToken() {
        long now = System.currentTimeMillis();
        if (cachedToken != null && tokenExpiresAt > now) {
            return cachedToken;
        }
        Map<?, ?> response = tokenClient.post().uri("/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", adminUsername, "password", adminPassword))
                .retrieve().body(Map.class);
        String token = response == null ? null : (String) response.get("access_token");
        if (token != null) {
            cachedToken = token;
            tokenExpiresAt = now + TOKEN_TTL_MS;
        }
        return token;
    }

    /** 401이면 캐시를 버리고 한 번만 재발급해 재시도한다(만료·회전 대응). */
    private <T> T withAuth(java.util.function.Function<String, T> call) {
        try {
            return call.apply(bearerToken());
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() == 401) {
                cachedToken = null;
                return call.apply(bearerToken());
            }
            throw ex;
        }
    }
}
