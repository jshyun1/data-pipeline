package com.company.pipeline.pipeline;

import com.company.pipeline.workflow.AirflowVariableClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * CDC 파이프라인 목록을 Airflow Variable로 내보낸다.
 *
 * <p>예전에는 {@code kafka_pipelines_dynamic.py}가 파싱될 때마다 이 백엔드의
 * {@code GET /api/pipelines}를 직접 호출해 DAG를 만들었다. 그래서 백엔드가 잠깐만
 * 안 떠 있어도 그 파싱 주기에 <b>CDC DAG가 통째로 사라졌고</b>, 대기 중이던
 * {@code monitor_cdc_runtime} 센서가 깨어나 자기 DAG를 못 찾고 죽었다.
 *
 * <pre>
 * pipeline-api 조회 실패, 이번 파싱 주기엔 Kafka 파이프라인 DAG를 생성하지 않음
 * Dag not found during start up
 * Startup reschedule limit exceeded
 * </pre>
 *
 * <p>실제로 2026-08-24에 그렇게 두 CDC DAG의 감시가 끊겼고, CDC는 멀쩡히 도는데
 * 화면에는 일주일 내내 «실패»로 남았다. ETL 워크플로우 팩토리가 같은 문제를 겪고
 * «Variable만 읽는다»로 해결했던 것과 같은 방식으로 CDC도 옮긴다.
 *
 * <p>변경 지점마다 갱신을 심지 않고 주기적으로 맞춘다. 생성·수정·삭제·이름변경이
 * 여러 경로에 흩어져 있어 한 곳이라도 빠지면 조용히 어긋나는데, 주기 동기화는
 * 무엇을 놓치든 다음 주기에 스스로 회복한다.
 */
@Component
public class CdcDagSpecPublisher {

    private static final Logger log = LoggerFactory.getLogger(CdcDagSpecPublisher.class);

    /** DAG 팩토리가 먼저 읽는 파이프라인 id 목록. */
    public static final String INDEX_KEY = "cdc_pipeline_index";
    /** 파이프라인별 spec. {@code cdc_pipeline_spec__{id}} */
    public static final String SPEC_PREFIX = "cdc_pipeline_spec__";

    private final JdbcTemplate jdbc;
    private final AirflowVariableClient variables;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 직전에 내보낸 내용. 같으면 Airflow를 두드리지 않는다(1분마다 쓰기를 반복할 이유가 없다). */
    private volatile String lastPublished = null;

    public CdcDagSpecPublisher(JdbcTemplate jdbc, AirflowVariableClient variables) {
        this.jdbc = jdbc;
        this.variables = variables;
    }

    public static String specKey(long pipelineId) {
        return SPEC_PREFIX + pipelineId;
    }

    /** 60초마다 맞춘다. 파이프라인 정의는 자주 바뀌지 않아 이 정도면 충분하다. */
    @Scheduled(initialDelay = 20_000, fixedDelay = 60_000)
    public void publish() {
        try {
            List<Map<String, Object>> rows = jdbc.queryForList("""
                    SELECT id, name, pipeline_type, target_schema, target_table
                    FROM pipeline_definition
                    WHERE pipeline_type IN ('TABLE_CDC', 'LOG_FILE')
                    ORDER BY id""");

            Set<Long> ids = new LinkedHashSet<>();
            Map<Long, String> specs = new LinkedHashMap<>();
            for (Map<String, Object> row : rows) {
                long id = ((Number) row.get("id")).longValue();
                ids.add(id);
                Map<String, Object> spec = new LinkedHashMap<>();
                spec.put("id", id);
                spec.put("name", row.get("name"));
                spec.put("pipeline_type", row.get("pipeline_type"));
                spec.put("target_schema", row.get("target_schema"));
                spec.put("target_table", row.get("target_table"));
                specs.put(id, objectMapper.writeValueAsString(spec));
            }

            String snapshot = objectMapper.writeValueAsString(specs);
            if (snapshot.equals(lastPublished)) {
                return;
            }

            for (Map.Entry<Long, String> entry : specs.entrySet()) {
                variables.upsert(specKey(entry.getKey()), entry.getValue());
            }
            // 인덱스를 마지막에 쓴다. spec보다 먼저 쓰면 팩토리가 «있다는데 없는» 항목을 만난다.
            variables.upsert(INDEX_KEY, objectMapper.writeValueAsString(ids));
            lastPublished = snapshot;
            log.info("CDC DAG spec 게시 - 파이프라인 {}개 {}", ids.size(), ids);
        } catch (Exception ex) {
            // 게시에 실패해도 CDC 자체는 돈다. 다음 주기에 다시 시도한다.
            log.warn("CDC DAG spec 게시 실패(다음 주기에 재시도): {}", ex.getMessage());
        }
    }

    /** 파이프라인을 만들거나 지운 직후 즉시 반영하고 싶을 때(주기 동기화와 별개로 호출 가능). */
    public void publishNow() {
        lastPublished = null;
        publish();
    }

    /** 지워진 파이프라인의 spec Variable을 치운다. 남아 있어도 인덱스에 없으면 무시되지만 지저분하다. */
    public void forget(long pipelineId) {
        try {
            variables.delete(specKey(pipelineId));
        } catch (Exception ex) {
            log.warn("CDC DAG spec 삭제 실패 - id={} : {}", pipelineId, ex.getMessage());
        }
        publishNow();
    }
}
