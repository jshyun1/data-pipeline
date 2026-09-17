package com.company.pipeline.llm.client;

/**
 * LLM 에 보내는 메시지 한 개. {@code role} 은 system · user · assistant 중 하나다.
 * 제공자마다 system 을 두는 자리가 달라(Anthropic 은 별도 필드, Gemini 는 systemInstruction) 여기서는 역할만 적고
 * 요청 조립은 {@link LlmChatClient} 가 제공자별로 한다.
 */
public record LlmMessage(String role, String content) {

    public static LlmMessage system(String content) {
        return new LlmMessage("system", content);
    }

    public static LlmMessage user(String content) {
        return new LlmMessage("user", content);
    }

    public static LlmMessage assistant(String content) {
        return new LlmMessage("assistant", content);
    }
}
