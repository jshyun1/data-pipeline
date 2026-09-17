package com.company.pipeline.llm.agent;

import com.company.pipeline.llm.client.LlmMessage;
import java.util.List;

/**
 * 속성창 GPT 탭의 대화가 길어졌을 때 오래된 문답을 줄이는 요약 요청(Claude Code 의 자동 요약과 같은 역할).
 * 대상 정보는 매번 따로 붙으므로 요약에 되풀이하지 않게 한다.
 */
public final class ConversationSummarizer {

    /** 요약 결과 상한. 요약이 다시 길이를 잡아먹지 않도록. */
    public static final int MAX_SUMMARY_CHARS = 2_000;
    public static final int MAX_OUTPUT_TOKENS = 1_024;

    static final String SYSTEM_PROMPT = """
            너는 대화 요약 담당이다. 운영자와 운영 도우미가 나눈 대화를 이어서 대화할 수 있게 요약한다.
            - 1,500자 이내 한국어로 쓴다.
            - 남길 것: 운영자가 물은 것, 확인된 사실과 결론, 제안된 조치와 그 결과, 아직 풀리지 않은 질문.
            - 대상의 설정 · SQL · 상태 원문은 다음 대화에 따로 전달되므로 되풀이하지 않는다.
            - 요약만 쓴다. 인사말이나 설명을 붙이지 않는다.
            """;

    private ConversationSummarizer() {
    }

    public static List<LlmMessage> messages(String previousSummary, List<LlmMessage> droppedTurns) {
        StringBuilder body = new StringBuilder();
        if (previousSummary != null && !previousSummary.isBlank()) {
            body.append("[기존 요약]\n").append(previousSummary.strip()).append("\n\n");
        }
        body.append("[요약할 대화]\n");
        for (LlmMessage turn : droppedTurns) {
            body.append("user".equals(turn.role()) ? "운영자: " : "도우미: ").append(turn.content().strip()).append("\n\n");
        }
        return List.of(LlmMessage.system(SYSTEM_PROMPT), LlmMessage.user(body.toString().strip()));
    }
}
