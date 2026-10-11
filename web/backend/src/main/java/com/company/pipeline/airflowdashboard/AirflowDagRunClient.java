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

    /**
     * 지금 돌고 있는 Run 만. «이 파이프라인을 이미 감시 중인가»를 볼 때 쓴다.
     *
     * <p>{@link #getDagRuns} 는 최근 50건이라, 감시 Run 이 오래전에 떴고 그 뒤로 워크플로우의 일괄 지시가
     * 건너뜀 Run 을 50건 넘게 쌓으면 창 밖으로 밀려나 «감시 없음»으로 오판한다. 상태로 걸러 받는다.
     */
    public List<DagRun> getRunningDagRuns(String dagId) {
        DagRunCollection response = withAuth(token -> restClient.get()
                .uri(uri -> uri.path("/dags/{dagId}/dagRuns")
                        .queryParam("state", "running")
                        .queryParam("limit", 100)
                        .queryParam("order_by", "start_date")
                        .build(dagId))
                .header("Authorization", "Bearer " + token)
                .retrieve()
                .body(DagRunCollection.class));
        if (response == null || response.dagRuns() == null) {
            return List.of();
        }
        return response.dagRuns().stream()
                .sorted(Comparator.comparing(DagRun::startDate, Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();
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


    /**
     * 모든 DAG 의 최근 실행. 워크플로우 알림 규칙이 «어느 워크플로우가 실패했나»를 볼 때 쓴다.
     *
     * <p>DAG 마다 따로 부르면 워크플로우 수만큼 호출이 늘어난다. Airflow 는 dagId 자리에 {@code ~}
     * 를 주면 전체를 한 번에 준다 - 실시간 모니터링 화면이 쓰는 것과 같은 경로라, 화면에 보이는
     * 것과 알림이 보는 것이 같아진다(우리 DB 에 사본을 두지 않는 이유).
     */
    public List<DagRunWithDag> getRecentDagRuns(int limit) {
        int capped = Math.max(1, Math.min(limit, 500));
        DagRunWithDagCollection response = withAuth(token -> restClient.get()
                .uri(uri -> uri.path("/dags/~/dagRuns")
                        .queryParam("limit", capped)
                        .queryParam("order_by", "-start_date")
                        .build())
                .header("Authorization", "Bearer " + token)
                .retrieve()
                .body(DagRunWithDagCollection.class));
        if (response == null || response.dagRuns() == null) {
            return List.of();
        }
        return response.dagRuns();
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    record DagRunWithDagCollection(@JsonProperty("dag_runs") List<DagRunWithDag> dagRuns) {
    }

    /** 전체 조회용 - 어느 DAG 의 실행인지가 함께 온다. */
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    public record DagRunWithDag(
            @JsonProperty("dag_id") String dagId,
            @JsonProperty("dag_run_id") String dagRunId,
            String state,
            @JsonProperty("start_date") OffsetDateTime startDate,
            @JsonProperty("end_date") OffsetDateTime endDate) {
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

    /**
     * DAG 의 일시 중지 상태를 바꾼다(화면 «해제» 버튼).
     *
     * <p>일시 중지를 <b>푸는 순간 밀린 회차가 곧바로 한 번 돈다</b> - catchup 을 꺼 두어도 Airflow 가
     * «빠진 가장 최근 회차»를 재개 직후 만든다(2026-09-11 · 09-16 실측). 부르는 쪽에서 그 점을 알리고 받아야 한다.
     */
    public void setPaused(String dagId, boolean paused) {
        withAuth(token -> restClient.patch()
                .uri("/dags/{dagId}", dagId)
                .header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("is_paused", paused))
                .retrieve()
                .toBodilessEntity());
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

    /**
     * 실행 한 건(2026-10-11, 워크플로우 로그 AI 어시스턴트의 «실행 차수» 대상). 없으면 null.
     * 목록({@link #getDagRuns})은 최근 50건이라 오래된 차수를 고르면 빠진다 - id 로 바로 받는다.
     */
    public RunDetail getDagRun(String dagId, String runId) {
        try {
            return withAuth(token -> restClient.get()
                    .uri("/dags/{dagId}/dagRuns/{runId}", dagId, runId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(RunDetail.class));
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() == 404) {
                return null;
            }
            throw ex;
        }
    }

    /** 실행 한 건의 task 들(워크플로우 노드 × 단계). 실행이 없으면 빈 목록. */
    public List<TaskInstance> getTaskInstances(String dagId, String runId) {
        try {
            TaskInstanceCollection response = withAuth(token -> restClient.get()
                    .uri(uri -> uri.path("/dags/{dagId}/dagRuns/{runId}/taskInstances")
                            .queryParam("limit", 500)
                            .build(dagId, runId))
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(TaskInstanceCollection.class));
            return response == null || response.taskInstances() == null ? List.of() : response.taskInstances();
        } catch (RestClientResponseException ex) {
            if (ex.getStatusCode().value() == 404) {
                return List.of();
            }
            throw ex;
        }
    }

    /**
     * task 로그의 마지막 {@code maxLines} 줄. 실패한 단계의 원인을 붙이는 데 쓴다 - 앞부분(시작 · 설정 줄)은 원인이 아니다.
     * 못 읽으면 null.
     */
    public String getTaskLogTail(String dagId, String runId, String taskId, int tryNumber, int maxLines) {
        try {
            com.fasterxml.jackson.databind.JsonNode body = withAuth(token -> restClient.get()
                    .uri(uri -> uri.path("/dags/{dagId}/dagRuns/{runId}/taskInstances/{taskId}/logs/{tryNumber}")
                            .queryParam("full_content", true)
                            .build(dagId, runId, taskId, Math.max(1, tryNumber)))
                    .header("Authorization", "Bearer " + token)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(com.fasterxml.jackson.databind.JsonNode.class));
            if (body == null || !body.path("content").isArray()) {
                return null;
            }
            List<String> lines = new ArrayList<>();
            body.path("content").forEach(entry -> {
                String text = entry.isTextual() ? entry.asText() : entry.path("event").asText("");
                if (entry.path("error_detail").isArray()) {
                    StringBuilder detail = new StringBuilder();
                    entry.path("error_detail").forEach(d -> detail.append(' ')
                            .append(d.path("exc_type").asText("")).append(": ").append(d.path("exc_value").asText("")));
                    text = text + detail;
                }
                if (!text.isBlank()) {
                    lines.add(text);
                }
            });
            return String.join("\n", lines.subList(Math.max(0, lines.size() - maxLines), lines.size()));
        } catch (RuntimeException ex) {
            return null;
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
            @JsonProperty("is_stale") Boolean stale,
            List<DagTag> tags) {

        public Dag(String dagId, String displayName, String description, Boolean stale) {
            this(dagId, displayName, description, stale, List.of());
        }
    }

    public record DagTag(String name) {
    }

    record DagRunCollection(@JsonProperty("dag_runs") List<DagRun> dagRuns) {
    }

    record TaskInstanceCollection(@JsonProperty("task_instances") List<TaskInstance> taskInstances) {
    }

    /** 실행 한 건의 자세한 값. {@link DagRun} 에 없는 실행 유형 · 대기열 시각을 같이 받는다. */
    public record RunDetail(
            @JsonProperty("dag_run_id") String dagRunId,
            @JsonProperty("dag_id") String dagId,
            String state,
            @JsonProperty("run_type") String runType,
            @JsonProperty("queued_at") OffsetDateTime queuedAt,
            @JsonProperty("start_date") OffsetDateTime startDate,
            @JsonProperty("end_date") OffsetDateTime endDate) {
    }

    public record TaskInstance(
            @JsonProperty("task_id") String taskId,
            String state,
            @JsonProperty("try_number") Integer tryNumber,
            @JsonProperty("start_date") OffsetDateTime startDate,
            @JsonProperty("end_date") OffsetDateTime endDate) {
    }

    public record DagRun(
            @JsonProperty("dag_run_id") String dagRunId,
            String state,
            @JsonProperty("start_date") OffsetDateTime startDate,
            @JsonProperty("end_date") OffsetDateTime endDate,
            /**
             * 수동 실행 때 넘긴 파라미터. CDC 제어 DAG는 여기에 action(deploy/start/stop/monitor)이
             * 들어온다 - "지금 도는 Run이 감시 Run인가"를 판정하는 유일한 근거다.
             */
            Map<String, Object> conf) {
    }
}
