package com.company.pipeline.llm.agent;

import com.company.pipeline.llm.client.LlmMessage;
import com.company.pipeline.llm.context.LlmTargetType;
import java.util.ArrayList;
import java.util.List;

/**
 * 속성창 GPT 탭(설계 5-2) - 선택한 대상(CDC 파이프라인 · 워크플로우 노드 · ETL job)에 대한 자유 질문.
 *
 * <p>첫 사용자 메시지 맨 앞에 대상 정보를 붙인다(요청마다 새로 모은 것). 대화가 길어지면 {@code ConversationFitter} 가 오래된 문답을
 * 요약하고, 그 요약도 대상 정보 바로 뒤에 붙인다 - 역할이 user · assistant 번갈아 이어지도록(Anthropic · Gemini 는 순서를 강제한다).
 */
public final class JobAnalysisAgent {

    public static final String SYSTEM_PROMPT = """
            너는 데이터 파이프라인 운영 플랫폼 «Cerebro ETL» 의 운영 도우미다. 운영자가 속성창에서 고른 대상(CDC 파이프라인 · 워크플로우 노드 ·
            ETL job)에 대해 질문한다. 구성 설명, SQL · 적재 방식 해석, 실행 순서와 영향, 개선점, 점검 방법 등을 답한다.

            """ + AgentPrompts.PLATFORM + """

            [규칙]
            """ + AgentPrompts.COMMON_RULES + """
            - 대상 정보는 질문 시점에 저장된 설계와 상태다. 화면에서 아직 저장하지 않은 변경은 들어 있지 않다.
            - 질문에 맞는 길이로 답한다. 정해진 형식은 없고, 절차는 번호 목록으로 쓴다.
            - [이전 대화 요약] 이 있으면 앞 대화의 결론으로 보고 이어서 답한다.
            """;

    private JobAnalysisAgent() {
    }

    /**
     * @param summary   앞 대화 요약(없으면 null)
     * @param keptTurns 남긴 문답(user 로 시작해 user · assistant 번갈아)
     * @param question  이번 질문
     */
    public static List<LlmMessage> messages(LlmTargetType targetType, String contextText, String summary,
                                            List<LlmMessage> keptTurns, String question) {
        StringBuilder head = new StringBuilder("[대상: ").append(ErrorAnalysisAgent.targetLabel(targetType)).append("]\n\n")
                .append(contextText);
        if (summary != null && !summary.isBlank()) {
            head.append("\n\n[이전 대화 요약]\n").append(summary.strip());
        }
        head.append("\n\n[질문]\n");

        List<LlmMessage> out = new ArrayList<>();
        out.add(LlmMessage.system(SYSTEM_PROMPT));
        List<LlmMessage> conversation = new ArrayList<>(keptTurns);
        conversation.add(LlmMessage.user(question.strip()));
        LlmMessage first = conversation.get(0);
        out.add(LlmMessage.user(head + first.content()));
        out.addAll(conversation.subList(1, conversation.size()));
        return out;
    }
}
