package com.company.pipeline.airflowdashboard;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

@Component
public class AirflowDagRunClient {

    // 서비스 토큰 캐시. Airflow JWT 만료는 30일(AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME)이지만 보수적으로 20분만 재사용.
    private static final long TOKEN_TTL_MS = 20 * 60 * 1000L;

    private final RestClient restClient;
    private final RestClient tokenClient;
    private final String adminUsername;
    private final String adminPassword;

    private volatile String cachedToken;
    private volatile long tokenExpiresAt;

    public AirflowDagRunClient(
            @Value("${airflow.base-url:http://airflow-apiserver:8080}") String baseUrl,
            @Value("${airflow.admin-username:}") String adminUsername,
            @Value("${airflow.admin-password:}") String adminPassword) {
        this.adminUsername = adminUsername;
        this.adminPassword = adminPassword;
        // 백엔드→Airflow 는 브라우저용 per-user 프록시(cerebroetl-ui/airflow, nginx auth_request)가 아니라
        // airflow-apiserver 에 직접 붙는다. 그 프록시는 세션쿠키 기반이라 서버-투-서버 호출이 401 로 막힌다(실측).
        // Airflow(uvicorn)는 JDK 기본 HTTP/2 요청을 거부하므로 HTTP/1.1 로 고정(AirflowUserSyncService 와 동일, 실측).
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(10));
        // Airflow 앱은 /airflow prefix 아래 마운트되어 REST API v2 는 /airflow/api/v2 다(실측).
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl + "/airflow/api/v2")
                .requestFactory(requestFactory)
                .build();
        this.tokenClient = RestClient.builder()
                .baseUrl(baseUrl + "/airflow")
                .requestFactory(requestFactory)
                .build();
    }

    /** Airflow 3 REST API v2 는 Bearer JWT 를 요구한다. 서비스 admin 계정으로 /auth/token 을 받아 캐시한다. */
    private synchronized String bearerToken() {
        long now = System.currentTimeMillis();
        if (cachedToken != null && tokenExpiresAt > now) {
            return cachedToken;
        }
        Map<?, ?> resp = tokenClient.post().uri("/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", adminUsername, "password", adminPassword))
                .retrieve().body(Map.class);
        String token = resp == null ? null : (String) resp.get("access_token");
        if (token != null) {
            cachedToken = token;
            tokenExpiresAt = now + TOKEN_TTL_MS;
        }
        return token;
    }

    /** 401 이면 캐시 토큰을 버리고 한 번만 재발급해 재시도한다(만료/회전 대응). */
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

    public List<DagRun> getDagRuns(String dagId) {
        DagRunCollection response = withAuth(token -> restClient.get()
                .uri(uri -> uri.path("/dags/{dagId}/dagRuns")
                        .queryParam("limit", 50)
                        .queryParam("order_by", "-start_date")
                        .build(dagId))
                .header("Authorization", "Bearer " + token)
                .retrieve()
                .body(DagRunCollection.class));
        if (response == null || response.dagRuns() == null) {
            return List.of();
        }
        return response.dagRuns().stream()
                .sorted(Comparator.comparing(DagRun::startDate, Comparator.nullsLast(Comparator.reverseOrder())))
                .toList();
    }

    public List<Dag> getDags() {
        int limit = 500;
        List<Dag> dags = new ArrayList<>();
        for (int offset = 0; ; offset += limit) {
            int pageOffset = offset;
            DagCollection response = withAuth(token -> restClient.get()
                    .uri(uri -> uri.path("/dags")
                            .queryParam("limit", limit)
                            .queryParam("offset", pageOffset)
                            .build())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(DagCollection.class));
            List<Dag> page = response == null || response.dags() == null ? List.of() : response.dags();
            dags.addAll(page);
            int total = response == null || response.totalEntries() == null ? dags.size() : response.totalEntries();
            if (page.size() < limit || dags.size() >= total) {
                return List.copyOf(dags);
            }
        }
    }

    /** DAG 를 즉시 실행(수동 트리거)한다. 감사·인가는 이 호출을 감싸는 pipeline-api 엔드포인트가 담당한다. */
    public DagRun triggerDag(String dagId, Map<String, Object> conf) {
        Map<String, Object> body = new HashMap<>();
        body.put("logical_date", null);
        body.put("conf", conf == null ? Map.of() : conf);
        return withAuth(token -> restClient.post()
                .uri("/dags/{dagId}/dagRuns", dagId)
                .header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(DagRun.class));
    }

    /** Airflow Variable upsert(없으면 POST, 있으면 409 → PATCH). 스케줄 저장에 쓴다. */
    public void upsertVariable(String key, String value) {
        try {
            withAuth(token -> restClient.post()
                    .uri("/variables")
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("key", key, "value", value))
                    .retrieve().toBodilessEntity());
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() != 409) {
                throw ex;
            }
            withAuth(token -> restClient.patch()
                    .uri("/variables/{key}", key)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("key", key, "value", value))
                    .retrieve().toBodilessEntity());
        }
    }

    public void deleteVariable(String key) {
        try {
            withAuth(token -> restClient.delete()
                    .uri("/variables/{key}", key)
                    .header("Authorization", "Bearer " + token)
                    .retrieve().toBodilessEntity());
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() != 404) {
                throw ex;
            }
        }
    }

    public void deleteDag(String dagId) {
        try {
            withAuth(token -> restClient.delete()
                    .uri("/dags/{dagId}", dagId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .toBodilessEntity());
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() != 404) {
                throw ex;
            }
        }
    }

    record DagCollection(List<Dag> dags, @JsonProperty("total_entries") Integer totalEntries) {
    }

    public record Dag(
            @JsonProperty("dag_id") String dagId,
            @JsonProperty("dag_display_name") String displayName,
            String description,
            @JsonProperty("is_stale") Boolean stale) {
    }

    record DagRunCollection(@JsonProperty("dag_runs") List<DagRun> dagRuns) {
    }

    public record DagRun(
            @JsonProperty("dag_run_id") String dagRunId,
            String state,
            @JsonProperty("start_date") OffsetDateTime startDate,
            @JsonProperty("end_date") OffsetDateTime endDate) {
    }
}
