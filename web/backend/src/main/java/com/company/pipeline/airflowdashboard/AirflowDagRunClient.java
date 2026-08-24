package com.company.pipeline.airflowdashboard;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

@Component
public class AirflowDagRunClient {

    private final RestClient restClient;

    public AirflowDagRunClient(
            @Value("${airflow.history-base-url:http://cerebroetl-ui/airflow}") String baseUrl) {
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(10));
        this.restClient = RestClient.builder()
                .baseUrl(baseUrl + "/api/v2")
                .requestFactory(requestFactory)
                .build();
    }

    public List<DagRun> getDagRuns(String dagId) {
        DagRunCollection response = restClient.get()
                .uri(uri -> uri.path("/dags/{dagId}/dagRuns")
                        .queryParam("limit", 50)
                        .queryParam("order_by", "-start_date")
                        .build(dagId))
                .retrieve()
                .body(DagRunCollection.class);
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
            DagCollection response = restClient.get()
                    .uri(uri -> uri.path("/dags")
                            .queryParam("limit", limit)
                            .queryParam("offset", pageOffset)
                            .build())
                    .retrieve()
                    .body(DagCollection.class);
            List<Dag> page = response == null || response.dags() == null ? List.of() : response.dags();
            dags.addAll(page);
            int total = response == null || response.totalEntries() == null ? dags.size() : response.totalEntries();
            if (page.size() < limit || dags.size() >= total) {
                return List.copyOf(dags);
            }
        }
    }

    public void deleteDag(String dagId) {
        try {
            restClient.delete()
                    .uri("/dags/{dagId}", dagId)
                    .retrieve()
                    .toBodilessEntity();
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

    public record DagRun(
            @JsonProperty("dag_run_id") String dagRunId,
            String state,
            @JsonProperty("start_date") OffsetDateTime startDate,
            @JsonProperty("end_date") OffsetDateTime endDate) {
    }
}
