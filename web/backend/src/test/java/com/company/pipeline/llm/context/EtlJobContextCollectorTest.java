package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EtlJobContextCollectorTest {

    private static final String ERROR = "PutDatabaseRecord ... TableNotFoundException: Table public.no_such_table not found";

    private EtlJobRepository jobRepository;
    private EtlJobContextCollector collector;

    @BeforeEach
    void setUp() {
        jobRepository = mock(EtlJobRepository.class);
        EtlJobContextText jobText = mock(EtlJobContextText.class);
        when(jobText.steps(any())).thenReturn("단계 없음");
        when(jobText.recentRuns(any(), isNull())).thenReturn("실행 없음");
        collector = new EtlJobContextCollector(jobRepository, jobText);
    }

    private static String sections(LlmContextCollector.CollectedTarget collected) {
        return collected.context().sections().stream()
                .map(section -> section.title() + ": " + section.body())
                .collect(Collectors.joining("\n"));
    }

    @Test
    void 지워진_job_도_해석한다_삭제_표시를_붙인다() {
        // 2026-10-06: 지워진 job 의 실패 로그를 해석하면 «찾을 수 없음» → 사유 없는 HTTP 500 이었다.
        EtlJob job = new EtlJob("pg-36", "_감시시험_DB실패");
        job.markDeleted();
        when(jobRepository.findById(36L)).thenReturn(Optional.of(job));

        LlmContextCollector.CollectedTarget collected = collector.collect(Map.of("jobId", "36"), ERROR);

        assertThat(collected.label()).isEqualTo("_감시시험_DB실패");
        assertThat(sections(collected)).contains("삭제된 job").contains("TableNotFoundException");
    }

    @Test
    void job_기록이_없으면_오류_내용만으로_해석한다() {
        when(jobRepository.findById(eq(99L))).thenReturn(Optional.empty());

        LlmContextCollector.CollectedTarget collected = collector.collect(Map.of("jobId", "99"), ERROR);

        assertThat(collected.targetId()).isEqualTo("jobId 99");
        assertThat(sections(collected)).contains("job 정보가 남아 있지 않습니다").contains("TableNotFoundException");
    }

    @Test
    void job_기록도_오류_내용도_없으면_찾을_수_없음() {
        when(jobRepository.findByNifiPgId("pg-x")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> collector.collect(Map.of("groupId", "pg-x"), null))
                .isInstanceOf(BusinessException.class)
                .satisfies(ex -> assertThat(((BusinessException) ex).getErrorCode()).isEqualTo(ErrorCode.LLM_TARGET_NOT_FOUND));
    }
}
