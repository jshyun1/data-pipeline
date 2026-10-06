package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.monitoring.RealtimePipelineMetricService;
import com.company.pipeline.pipeline.PipelineCommandHistoryRepository;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineGroupRepository;
import com.company.pipeline.pipeline.PipelineMetadataArchive;
import com.company.pipeline.pipeline.PipelineMetadataArchiveRepository;
import com.company.pipeline.pipeline.PipelineRuntimeStatusService;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CdcPipelineContextCollectorTest {

    @Test
    void summarizeTrace_keepsHeadAndCausedByOnly() {
        StringBuilder trace = new StringBuilder("org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask\n");
        for (int i = 0; i < 200; i++) {
            trace.append("\tat org.apache.kafka.Frame").append(i).append("(Frame.java:1)\n");
        }
        trace.append("Caused by: java.sql.BatchUpdateException: value too long for type character varying(10)\n");
        trace.append("\tat org.postgresql.Frame(Frame.java:1)\n");

        String summary = CdcPipelineContextCollector.summarizeTrace(trace.toString());

        assertThat(summary).startsWith("org.apache.kafka.connect.errors.ConnectException")
                .contains("Caused by: java.sql.BatchUpdateException: value too long")
                .doesNotContain("Frame199");
        assertThat(summary.lines().count()).isLessThan(20);
    }

    private static final String ERROR = "org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.";

    private final PipelineDefinitionRepository pipelineRepository = mock(PipelineDefinitionRepository.class);
    private final PipelineMetadataArchiveRepository archiveRepository = mock(PipelineMetadataArchiveRepository.class);
    private final CdcPipelineContextCollector collector = new CdcPipelineContextCollector(pipelineRepository,
            mock(PipelineGroupRepository.class), mock(PipelineConnectorRepository.class), mock(PipelineCommandHistoryRepository.class),
            mock(PipelineRuntimeStatusService.class), mock(KafkaConnectClient.class), mock(RealtimePipelineMetricService.class),
            archiveRepository);

    private static String sections(LlmContextCollector.CollectedTarget collected) {
        return collected.context().sections().stream()
                .map(section -> section.title() + ": " + section.body())
                .collect(Collectors.joining("\n"));
    }

    @Test
    void 지운_파이프라인은_보관_메타로_해석한다_삭제_표시를_붙인다() {
        // 2026-10-06: CDC 로그에 남은 지운 파이프라인의 실패는 «찾을 수 없음»이라 화면이 해석 단추를 숨겼다.
        when(pipelineRepository.findById(16L)).thenReturn(Optional.empty());
        when(archiveRepository.findById(16L)).thenReturn(Optional.of(new PipelineMetadataArchive(16L, "e2e-dash", "TABLE_CDC",
                "POSTGRESQL · e2e_dash.src_d", "POSTGRESQL · cdc_landing.e2e_dash_tgt")));

        LlmContextCollector.CollectedTarget collected = collector.collect(Map.of("pipelineId", "16"), ERROR);

        assertThat(collected.label()).isEqualTo("e2e-dash");
        assertThat(sections(collected)).contains("삭제된 파이프라인").contains("cdc_landing.e2e_dash_tgt").contains("WorkerSinkTask");
    }

    @Test
    void 보관_메타도_없으면_오류_내용만으로_해석한다() {
        when(pipelineRepository.findById(6L)).thenReturn(Optional.empty());
        when(archiveRepository.findById(6L)).thenReturn(Optional.empty());

        LlmContextCollector.CollectedTarget collected = collector.collect(Map.of("pipelineId", "6"), ERROR);

        assertThat(collected.label()).isEqualTo("pipelineId 6");
        assertThat(sections(collected)).contains("파이프라인 정보가 남아 있지 않습니다").contains("WorkerSinkTask");
    }

    @Test
    void 보관_메타도_오류_내용도_없으면_찾을_수_없음() {
        when(pipelineRepository.findById(6L)).thenReturn(Optional.empty());
        when(archiveRepository.findById(6L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> collector.collect(Map.of("pipelineId", "6"), null))
                .isInstanceOf(BusinessException.class)
                .satisfies(ex -> assertThat(((BusinessException) ex).getErrorCode()).isEqualTo(ErrorCode.LLM_TARGET_NOT_FOUND));
    }
}
