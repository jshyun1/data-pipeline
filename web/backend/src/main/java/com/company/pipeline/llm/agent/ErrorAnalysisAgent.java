package com.company.pipeline.llm.agent;

import com.company.pipeline.llm.client.LlmMessage;
import com.company.pipeline.llm.context.LlmTargetType;
import java.util.ArrayList;
import java.util.List;

/**
 * 오류 분석(설계 5-3). 첫 답은 형식을 고정하고(원인 / 조치 방법 / 확인할 것 - 2026-10-02 순서 변경), 이어지는 추가 질문은 자유롭게 답한다.
 *
 * <p>«확신도»는 2026-09-28 에 뺐다 - 모델이 스스로 매기는 값이라 근거가 없고, «높음»이 붙었다고 더 맞지도 않다.
 * 판단 근거는 «원인» 안에서 로그 문구를 인용하게 하는 쪽이 낫다.
 *
 * <p>대화는 서버에 저장하지 않는다. 화면이 이번 창의 대화를 매번 같이 보내고, 서버는 대상 정보를 매번 새로 모아 맨 앞에 둔다 -
 * 그래서 추가 질문 때도 최신 상태(커넥터가 그새 다시 멈췄는지 등)를 본다.
 *
 * <p>대화가 길어지면 {@code ConversationFitter} 가 <b>오래된 추가 문답부터 뺀다</b>. 대상 정보와 첫 분석은 항상 남는다.
 */
public final class ErrorAnalysisAgent {

    public static final String SYSTEM_PROMPT = """
            너는 데이터 파이프라인 운영 플랫폼 «Cerebro ETL» 의 오류 분석 도우미다. 운영자가 로그 화면에서 실패를 보고 원인과 조치를 묻는다.

            """ + AgentPrompts.PLATFORM + """

            [규칙]
            """ + AgentPrompts.COMMON_RULES + """
            - 확인이 필요한 것은 «확인할 것» 으로 돌린다. 제목 이름을 항목 안에 다시 쓰지 않는다.

            [첫 답변 형식] 아래 세 제목을 이 순서로 그대로 쓴다.
            원인
            (한두 문장. 로그의 어느 문구가 근거인지 인용한다)

            조치 방법
            1. …

            확인할 것
            1. …

            [추가 질문] 형식에 얽매이지 말고 질문에만 간결히 답한다.
            """;

    private ErrorAnalysisAgent() {
    }

    /**
     * @param contextText  수집기가 모으고 (필요하면) 마스킹한 대상 정보
     * @param firstAnswer  첫 분석 답. 첫 요청이면 null
     * @param keptTurns    남긴 추가 문답(user · assistant 번갈아). 오래된 것은 이미 빠져 있다
     * @param droppedPairs 길이 제한으로 뺀 추가 문답 수
     * @param question     이번 추가 질문. 첫 요청이면 null
     */
    public static List<LlmMessage> messages(LlmTargetType targetType, String contextText, String firstAnswer,
                                            List<LlmMessage> keptTurns, int droppedPairs, String question) {
        List<LlmMessage> out = new ArrayList<>();
        out.add(LlmMessage.system(SYSTEM_PROMPT));
        out.add(LlmMessage.user("[분석 대상: " + targetLabel(targetType) + "]\n\n" + contextText
                + "\n\n위 실패의 원인과 조치를 첫 답변 형식으로 분석해 줘."));
        if (firstAnswer != null) {
            out.add(LlmMessage.assistant(firstAnswer));
        }
        out.addAll(keptTurns);
        if (question != null && !question.isBlank()) {
            String note = droppedPairs > 0
                    ? "(참고: 앞선 추가 질문 " + droppedPairs + "개와 그 답은 길이 제한으로 빠졌다)\n" : "";
            out.add(LlmMessage.user(note + question.strip()));
        }
        return out;
    }

    static String targetLabel(LlmTargetType type) {
        return switch (type) {
            case ETL_ASSISTANT -> "ETL 관리 어시스턴트";
            case WORKFLOW_TASK -> "워크플로우 실행 중 실패한 작업";
            case CDC_PIPELINE -> "CDC 파이프라인";
            case ETL_JOB -> "ETL job";
            case WORKFLOW -> "워크플로우";
            case WORKFLOW_DRAFT -> "작성 중인 새 워크플로우(저장 전)";
            case WORKFLOW_NODE -> "워크플로우 노드";
            case WORKFLOW_RUN -> "워크플로우 실행 차수";
        };
    }
}
