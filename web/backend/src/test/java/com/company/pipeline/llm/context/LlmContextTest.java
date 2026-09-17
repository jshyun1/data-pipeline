package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LlmContextTest {

    @Test
    void fitsEverything_keepsInsertionOrder() {
        LlmContext context = new LlmContext()
                .add("대상", "job A", LlmContext.PRIORITY_IDENTITY)
                .add("오류", "ORA-12170", LlmContext.PRIORITY_ERROR)
                .add("빈 구역", "  ", LlmContext.PRIORITY_DETAIL);

        LlmContext.Assembled assembled = context.assemble(10_000);

        assertThat(assembled.truncated()).isFalse();
        assertThat(assembled.text()).isEqualTo("## 대상\njob A\n\n## 오류\nORA-12170");
    }

    @Test
    void overLimit_errorFirst_thenDetailIsCut_historyDropped() {
        String error = "E".repeat(300);
        String sql = "S".repeat(2_000);
        String history = "H".repeat(1_000);
        LlmContext context = new LlmContext()
                .add("실행 이력", history, LlmContext.PRIORITY_HISTORY)
                .add("SQL", sql, LlmContext.PRIORITY_DETAIL)
                .add("오류", error, LlmContext.PRIORITY_ERROR);

        LlmContext.Assembled assembled = context.assemble(1_000);

        assertThat(assembled.truncated()).isTrue();
        assertThat(assembled.text()).contains(error).contains("길이 제한으로 뒷부분 생략").doesNotContain("HHHH");
        assertThat(assembled.omittedTitles()).containsExactly("실행 이력");
        assertThat(assembled.text().length()).isLessThanOrEqualTo(1_000);
        // 출력 순서는 추가 순서(실행 이력 → SQL → 오류)에서 빠진 것만 뺀 것
        assertThat(assembled.text().indexOf("## SQL")).isLessThan(assembled.text().indexOf("## 오류"));
    }
}
