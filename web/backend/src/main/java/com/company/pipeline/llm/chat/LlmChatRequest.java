package com.company.pipeline.llm.chat;

import com.company.pipeline.llm.agent.LlmAgentType;
import com.company.pipeline.llm.context.LlmTargetType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;

/**
 * GPT 분석 요청. 화면은 «대상 종류 + 대상 식별자 + 화면에 보이는 오류 + 이번 창의 대화» 만 보낸다(설계 5-2).
 * 대상 정보는 서버가 모은다.
 *
 * @param target    WORKFLOW_TASK: dagId · runId · taskId / CDC_PIPELINE: pipelineId
 * @param errorText 화면에 보이는 오류 문구(실패 로그 창의 실패 원인 · Airflow 기록, CDC 로그의 메시지)
 * @param history   이번 창에서 이미 오간 대화. 오류 분석은 첫 분석 답부터, 속성창 GPT 탭은 첫 질문부터.
 *                  속성창 대화가 요약된 뒤에는 요약에 들어간 부분을 빼고 보낸다
 * @param summary   속성창 GPT 탭의 앞 대화 요약(서버가 {@code compacted} 이벤트로 준 것). 없으면 비어 있다
 * @param question  이번 질문. 오류 분석의 첫 요청이면 비어 있다
 * @param contextOverride 화면의 [프롬프트] 창에서 운영자가 고쳐 보낸 대상 정보. 있으면 서버가 모은 것 대신 이것을 보낸다
 *                  (수집 · 권한 확인은 그대로 하고 본문만 바꾼다). 길이 제한 · 마스킹도 서버가 다시 건다
 * @param draft     WORKFLOW_DRAFT 전용 - 화면의 작성 중 설계(JSON: 속성 입력 · 노드 · 연결). 서버에 아직 없는 대상이라 화면이 보낸다.
 *                  이름 · 단계 같은 나머지는 서버가 id 로 찾아 붙인다
 */
public record LlmChatRequest(
        @NotNull LlmAgentType agentType,
        @NotNull LlmTargetType targetType,
        @NotEmpty Map<String, @Size(max = 500) String> target,
        @Size(max = 200_000) String errorText,
        @Valid @Size(max = 400) List<Turn> history,
        @Size(max = 10_000) String summary,
        @Size(max = 4_000) String question,
        @Size(max = 100_000) String draft,
        @Size(max = 200_000) String contextOverride) {

    public record Turn(
            @NotNull @Pattern(regexp = "user|assistant") String role,
            @NotBlank @Size(max = 30_000) String content) {
    }
}
