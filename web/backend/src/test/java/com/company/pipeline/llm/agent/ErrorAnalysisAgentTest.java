package com.company.pipeline.llm.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.llm.client.LlmMessage;
import com.company.pipeline.llm.context.LlmTargetType;
import java.util.List;
import org.junit.jupiter.api.Test;

class ErrorAnalysisAgentTest {

    @Test
    void firstRequest_isSystemThenContext() {
        List<LlmMessage> messages = ErrorAnalysisAgent.messages(LlmTargetType.CDC_PIPELINE, "## 오류\nX", null, List.of(), 0, null);

        assertThat(messages).extracting(LlmMessage::role).containsExactly("system", "user");
        assertThat(messages.get(1).content()).contains("CDC 파이프라인", "## 오류\nX", "첫 답변 형식");
        assertThat(messages.get(0).content()).contains("[플랫폼 구성]", "원인 추정", "확신도");
    }

    @Test
    void followUp_keepsFirstAnswer_andNotesDroppedPairs() {
        List<LlmMessage> kept = List.of(LlmMessage.user("q2"), LlmMessage.assistant("a2"));

        List<LlmMessage> messages = ErrorAnalysisAgent.messages(LlmTargetType.WORKFLOW_TASK, "ctx", "원인 추정 …", kept, 1, "재시도해도 돼?");

        assertThat(messages).extracting(LlmMessage::role).containsExactly("system", "user", "assistant", "user", "assistant", "user");
        assertThat(messages.get(2).content()).isEqualTo("원인 추정 …");
        assertThat(messages.get(5).content()).contains("1개", "재시도해도 돼?");
    }
}
