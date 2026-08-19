package com.company.pipeline.airflowdashboard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.airflowdashboard.AirflowDagRunClient.Dag;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

@ExtendWith(MockitoExtension.class)
class AirflowDagCatalogSyncServiceTest {

    @Mock
    private AirflowDagRunClient airflowClient;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    void reconcilesNewAndRemovedBusinessDags() {
        when(airflowClient.getDags()).thenReturn(List.of(
                new Dag("nifi_pipeline_abcd1234_control", "ETL 주문", "주문 적재", false),
                new Dag("nifi_pipeline_abcd1234_control", "중복", null, false),
                new Dag("kafka_pipeline_7_control", "CDC 고객", "고객 CDC", false),
                new Dag("test_hello", "테스트", null, false),
                new Dag("nifi_pipeline_stale000_control", "오래된 DAG", null, true)));
        when(jdbcTemplate.queryForList(anyString(), eq(String.class)))
                .thenReturn(
                        List.of("kafka_pipeline_7_control"),
                        List.of("kafka_pipeline_7_control", "kafka_pipeline_9_control"));
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1, 1});

        var result = new AirflowDagCatalogSyncService(airflowClient, jdbcTemplate).synchronize();

        assertThat(result.discoveredCount()).isEqualTo(3);
        assertThat(result.eligibleCount()).isEqualTo(2);
        assertThat(result.createdCount()).isEqualTo(1);
        assertThat(result.createdDagIds()).containsExactly("nifi_pipeline_abcd1234_control");
        assertThat(result.disabledCount()).isEqualTo(1);
        assertThat(result.disabledDagIds()).containsExactly("kafka_pipeline_9_control");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Object[]>> argsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbcTemplate, times(2)).batchUpdate(anyString(), argsCaptor.capture());
        assertThat(argsCaptor.getAllValues().get(0)).hasSize(2);
        assertThat(argsCaptor.getAllValues().get(0).get(0)).containsExactly(
                "nifi_pipeline_abcd1234_control", "ETL", "NiFi", "ETL 주문", "주문 적재");
        assertThat(argsCaptor.getAllValues().get(0).get(1)).containsExactly(
                "kafka_pipeline_7_control", "CDC", "Kafka", "CDC 고객", "고객 CDC");
        assertThat(argsCaptor.getAllValues().get(1).get(0)).containsExactly("kafka_pipeline_9_control");
    }
}
