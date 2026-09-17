package com.company.pipeline.llm.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.llm.client.LlmMessage;
import com.company.pipeline.llm.context.LlmTargetType;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobAnalysisAgentTest {

    @Test
    void firstQuestion_isPrefixedWithContext() {
        List<LlmMessage> messages = JobAnalysisAgent.messages(LlmTargetType.WORKFLOW_NODE, "## 노드\njob A", null, List.of(), "이 job 은 뭐 해?");

        assertThat(messages).extracting(LlmMessage::role).containsExactly("system", "user");
        assertThat(messages.get(1).content()).startsWith("[대상: 워크플로우 노드]").contains("## 노드\njob A", "[질문]\n이 job 은 뭐 해?")
                .doesNotContain("이전 대화 요약");
    }

    @Test
    void summaryAndOldestKeptQuestionShareFirstMessage_rolesAlternate() {
        List<LlmMessage> kept = List.of(LlmMessage.user("q3"), LlmMessage.assistant("a3"));

        List<LlmMessage> messages = JobAnalysisAgent.messages(LlmTargetType.CDC_PIPELINE, "ctx", "앞에서 lag 원인을 확인함", kept, "q4");

        assertThat(messages).extracting(LlmMessage::role).containsExactly("system", "user", "assistant", "user");
        assertThat(messages.get(1).content()).contains("ctx", "[이전 대화 요약]\n앞에서 lag 원인을 확인함", "[질문]\nq3");
        assertThat(messages.get(3).content()).isEqualTo("q4");
    }
}
