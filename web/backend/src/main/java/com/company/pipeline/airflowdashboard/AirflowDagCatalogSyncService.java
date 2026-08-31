package com.company.pipeline.airflowdashboard;

import com.company.pipeline.airflowdashboard.AirflowDagRunClient.Dag;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class AirflowDagCatalogSyncService {

    private static final Logger log = LoggerFactory.getLogger(AirflowDagCatalogSyncService.class);
    private static final Pattern FOLDER_TAG = Pattern.compile("^(?:folder|business|업무)[:=](.+)$", Pattern.CASE_INSENSITIVE);

    private static final String UPSERT_SQL = """
            INSERT INTO airflow_dag_catalog
                (dag_id, business_group, business_folder, display_name, description, sort_order, enabled)
            VALUES (?, ?, ?, ?, ?, 0, TRUE)
            ON CONFLICT (dag_id) DO UPDATE SET
                business_group = EXCLUDED.business_group,
                business_folder = EXCLUDED.business_folder,
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                enabled = TRUE,
                updated_at = now()
            """;
    private static final String ALL_IDS_SQL = "SELECT dag_id FROM airflow_dag_catalog";
    private static final String ENABLED_IDS_SQL = """
            SELECT dag_id FROM airflow_dag_catalog
            WHERE enabled = TRUE AND business_group IN ('CDC', 'ETL')
            """;
    private static final String DISABLE_SQL = """
            UPDATE airflow_dag_catalog SET enabled = FALSE, updated_at = now() WHERE dag_id = ?
            """;

    private final AirflowDagRunClient airflowClient;
    private final JdbcTemplate jdbcTemplate;

    public AirflowDagCatalogSyncService(AirflowDagRunClient airflowClient, JdbcTemplate jdbcTemplate) {
        this.airflowClient = airflowClient;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Scheduled(
            fixedDelayString = "${airflow.catalog-sync.interval-millis:30000}",
            initialDelayString = "${airflow.catalog-sync.initial-delay-millis:30000}")
    public void scheduledSync() {
        try {
            synchronize();
        } catch (RuntimeException ex) {
            log.warn("Airflow DAG 카탈로그 동기화 실패(다음 주기에 재시도): {}", ex.getMessage());
        }
    }

    public SyncResult synchronize() {
        Map<String, Dag> discovered = new LinkedHashMap<>();
        for (Dag dag : airflowClient.getDags()) {
            if (dag.dagId() != null && !dag.dagId().isBlank() && !Boolean.TRUE.equals(dag.stale())) {
                discovered.putIfAbsent(dag.dagId(), dag);
            }
        }

        List<Candidate> candidates = discovered.values().stream()
                .map(AirflowDagCatalogSyncService::candidate)
                .filter(candidate -> candidate != null)
                .toList();

        var candidateIds = candidates.stream().map(Candidate::dagId).collect(java.util.stream.Collectors.toSet());
        var existingIds = new HashSet<>(jdbcTemplate.queryForList(ALL_IDS_SQL, String.class));
        List<String> disabledDagIds = jdbcTemplate.queryForList(ENABLED_IDS_SQL, String.class).stream()
                .filter(dagId -> !candidateIds.contains(dagId))
                .toList();

        List<Object[]> upsertArgs = candidates.stream()
                .map(candidate -> new Object[]{
                        candidate.dagId(), candidate.businessGroup(), candidate.businessFolder(),
                        candidate.displayName(), candidate.description()})
                .toList();
        if (!upsertArgs.isEmpty()) {
            jdbcTemplate.batchUpdate(UPSERT_SQL, upsertArgs);
        }
        if (!disabledDagIds.isEmpty()) {
            jdbcTemplate.batchUpdate(DISABLE_SQL,
                    disabledDagIds.stream().map(dagId -> new Object[]{dagId}).toList());
        }
        List<String> createdDagIds = candidates.stream()
                .map(Candidate::dagId)
                .filter(dagId -> !existingIds.contains(dagId))
                .toList();
        return new SyncResult(
                discovered.size(), candidates.size(), createdDagIds.size(), createdDagIds,
                disabledDagIds.size(), disabledDagIds);
    }

    private static Candidate candidate(Dag dag) {
        String group;
        String folder;
        // etl_wf_*는 워크플로우 캔버스가 게시한 DAG다. 카탈로그에 넣어야 실행 현황 화면과
        // 이상 감지(AirflowDagAlert)가 이 워크플로우를 자기 대상으로 인식한다.
        if (dag.dagId().startsWith("etl_wf_")) {
            group = "ETL";
            folder = folder(dag, "워크플로우");
        } else if (dag.dagId().startsWith("nifi_pipeline_") && dag.dagId().endsWith("_control")) {
            group = "ETL";
            folder = folder(dag, "ETL");
        } else if (dag.dagId().startsWith("kafka_pipeline_") && dag.dagId().endsWith("_control")) {
            group = "CDC";
            folder = folder(dag, "Kafka");
        } else {
            return null;
        }
        String displayName = dag.displayName() == null || dag.displayName().isBlank()
                ? dag.dagId()
                : dag.displayName();
        return new Candidate(dag.dagId(), group, folder, displayName, dag.description());
    }

    private static String folder(Dag dag, String fallback) {
        if (dag.tags() != null) {
            for (var tag : dag.tags()) {
                var matcher = FOLDER_TAG.matcher(tag.name() == null ? "" : tag.name().trim());
                if (matcher.matches() && !matcher.group(1).isBlank()) {
                    return matcher.group(1).trim();
                }
            }
        }
        return fallback;
    }

    record Candidate(
            String dagId,
            String businessGroup,
            String businessFolder,
            String displayName,
            String description) {
    }

    public record SyncResult(
            int discoveredCount,
            int eligibleCount,
            int createdCount,
            List<String> createdDagIds,
            int disabledCount,
            List<String> disabledDagIds) {
    }
}
