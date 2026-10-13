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
    void draftTarget_swapsSavedRuleForDraftRuleAndAddsReviewGuide() {
        String prompt = JobAnalysisAgent.systemPrompt(LlmTargetType.WORKFLOW_DRAFT);

        assertThat(JobAnalysisAgent.SYSTEM_PROMPT).contains(JobAnalysisAgent.SAVED_RULE);
        assertThat(prompt).doesNotContain(JobAnalysisAgent.SAVED_RULE).contains(JobAnalysisAgent.DRAFT_RULE, "[작성 중인 새 워크플로우]");
        assertThat(JobAnalysisAgent.systemPrompt(LlmTargetType.WORKFLOW)).isEqualTo(JobAnalysisAgent.SYSTEM_PROMPT);

        List<LlmMessage> messages = JobAnalysisAgent.messages(LlmTargetType.WORKFLOW_DRAFT, "ctx", null, List.of(), "봐 줘");
        // 질문이 있으면 범위 안내가 붙는다(2026-10-13). 초안은 저장 전 설계를 받으므로 «저장하지 않은 변경»은 «받지 않은 것»이 아니다.
        assertThat(messages.get(0).content()).startsWith(prompt).contains(AnswerScope.MARKER + " 이 AI 어시스턴트는 작성 중인 새 워크플로우에 대해")
                .doesNotContain("화면에서 아직 저장하지 않은 변경");
        assertThat(messages.get(1).content()).startsWith("[대상: 작성 중인 새 워크플로우(저장 전)]");
    }

    @Test
    void summaryAndOldestKeptQuestionShareFirstMessage_rolesAlternate() {
        List<LlmMessage> kept = List.of(LlmMessage.user("q3"), LlmMessage.assistant("a3"));

        List<LlmMessage> messages = JobAnalysisAgent.messages(LlmTargetType.CDC_PIPELINE, "ctx", "앞에서 lag 원인을 확인함", kept, "q4");

        assertThat(messages).extracting(LlmMessage::role).containsExactly("system", "user", "assistant", "user");
        assertThat(messages.get(1).content()).contains("ctx", "[이전 대화 요약]\n앞에서 lag 원인을 확인함", "[질문]\nq3");
        assertThat(messages.get(3).content()).isEqualTo("q4");
    }

    @Test
    void question_addsAnswerScopeWithTargetSubjectAndSummary() {
        // 정확히 답할 수 없는 질문에는 답 첫머리에서 «무엇을 어디까지 답할 수 있는지»를 밝히게 한다(2026-10-13 요청).
        List<LlmMessage> messages = JobAnalysisAgent.messages(LlmTargetType.CDC_PIPELINE, "ctx", null, List.of(), "어제 몇 건 들어왔어?");

        String system = messages.get(0).content();
        assertThat(system).startsWith(JobAnalysisAgent.SYSTEM_PROMPT)
                .contains("[답변 범위]", AnswerScope.MARKER + " 이 AI 어시스턴트는 이 CDC 파이프라인에 대해 구성(소스 → 타깃",
                        "최근 제어 명령 8건", "화면에서 아직 저장하지 않은 변경", "범위 안에서 정확히 답할 수 있는 질문에는 이 문단을 쓰지 않는다");
        assertThat(JobAnalysisAgent.instructions(LlmTargetType.CDC_PIPELINE, true)).isEqualTo(system);
    }

    @Test
    void firstOpenSummary_hasNoAnswerScope() {
        // 탭을 열 때의 첫 요약은 형식이 정해져 있다 - 범위 안내를 붙이지 않는다.
        List<LlmMessage> messages = JobAnalysisAgent.messages(LlmTargetType.WORKFLOW_RUN, "ctx", null, List.of(), null);

        assertThat(messages.get(0).content()).isEqualTo(JobAnalysisAgent.systemPrompt(LlmTargetType.WORKFLOW_RUN))
                .doesNotContain(AnswerScope.MARKER);
        assertThat(JobAnalysisAgent.instructions(LlmTargetType.WORKFLOW_RUN, false)).isEqualTo(messages.get(0).content());
    }

    @Test
    void everyTargetType_hasAnswerScope() {
        // 대상 종류를 새로 만들면 범위도 같이 정해야 한다 - 빠지면 질문하는 순간 실패하므로 여기서 잡는다.
        for (LlmTargetType type : LlmTargetType.values()) {
            AnswerScope.Scope scope = AnswerScope.scope(type);
            assertThat(scope.subject()).as(type.name()).isNotBlank();
            assertThat(AnswerScope.guide(type)).as(type.name())
                    .contains(AnswerScope.MARKER + " 이 AI 어시스턴트는 " + scope.subject() + "에 대해 " + scope.summary() + "까지 답할 수 있습니다.");
        }
    }
}
