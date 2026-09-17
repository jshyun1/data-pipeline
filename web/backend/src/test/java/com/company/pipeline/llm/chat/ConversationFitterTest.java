package com.company.pipeline.llm.chat;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.llm.client.LlmMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConversationFitterTest {

    private static LlmMessage u(String text) {
        return LlmMessage.user(text);
    }

    private static LlmMessage a(String text) {
        return LlmMessage.assistant(text);
    }

    @Test
    void everythingFits_nothingDropped() {
        var normalized = ConversationFitter.normalize(List.of(u("q1"), a("a1"), u("q2"), a("a2")), false);

        var plan = ConversationFitter.fit(normalized, 100, 10_000);

        assertThat(plan.dropped()).isEmpty();
        assertThat(plan.droppedTurns()).isZero();
        assertThat(plan.keptTurns()).extracting(LlmMessage::content).containsExactly("q1", "a1", "q2", "a2");
    }

    @Test
    void overLimit_dropsOldestPairsFirst_andReportsRawIndex() {
        String big = "x".repeat(400);
        var normalized = ConversationFitter.normalize(List.of(u("q1"), a(big), u("q2"), a(big), u("q3"), a("a3")), false);

        // 고정 100 + 제한 600 → 남는 자리 500: 최신(q3,a3 = 4자)와 q2 쌍(402자)까지만 들어간다
        var plan = ConversationFitter.fit(normalized, 100, 600);

        assertThat(plan.dropped()).hasSize(1);
        assertThat(plan.keptTurns()).extracting(LlmMessage::content).containsExactly("q2", big, "q3", "a3");
        assertThat(plan.droppedTurns()).isEqualTo(2);
    }

    @Test
    void errorAnalysis_firstAnswerIsAlwaysKept() {
        String big = "y".repeat(1_000);
        var normalized = ConversationFitter.normalize(List.of(a("원인 추정"), u("q1"), a(big), u("q2"), a("a2")), true);

        var plan = ConversationFitter.fit(normalized, 0, 200);

        assertThat(plan.firstAnswer()).isEqualTo("원인 추정");
        assertThat(plan.keptTurns()).extracting(LlmMessage::content).containsExactly("q2", "a2");
        assertThat(plan.dropped()).hasSize(1);
    }

    @Test
    void unansweredQuestionsAreSkipped_soRolesStillAlternate() {
        // q1 은 답을 못 받았다(도중 실패) → 쌍이 아니다
        var normalized = ConversationFitter.normalize(List.of(u("q1"), u("q2"), a("a2"), a("stray")), false);

        assertThat(normalized.pairs()).hasSize(1);
        assertThat(normalized.pairs().get(0).startIndex()).isEqualTo(1);
    }

    @Test
    void nothingFits_allDropped_droppedTurnsIsWholeHistory() {
        var normalized = ConversationFitter.normalize(List.of(u("q1"), a("a1")), false);

        var plan = ConversationFitter.fit(normalized, 1_000, 500);

        assertThat(plan.kept()).isEmpty();
        assertThat(plan.droppedTurns()).isEqualTo(2);
    }
}
