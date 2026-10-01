package com.company.pipeline.llm.client;

import java.util.List;

/**
 * LLM 에 보내는 메시지 한 개. {@code role} 은 system · user · assistant · tool 중 하나다.
 * 제공자마다 system 을 두는 자리가 달라(Anthropic 은 별도 필드, Gemini 는 systemInstruction) 여기서는 역할만 적고
 * 요청 조립은 {@link LlmChatClient} 가 제공자별로 한다.
 *
 * <p>도구 호출(2026-10-02): assistant 가 도구를 불렀으면 {@code toolCalls} 가, 그 결과를 돌려주는 메시지는
 * role=tool · {@code toolCallId} 가 채워진다. OpenAI 형식(OpenAI · OpenAI 호환)에서만 쓴다.
 */
public record LlmMessage(String role, String content, List<ToolCall> toolCalls, String toolCallId) {

    public LlmMessage(String role, String content) {
        this(role, content, null, null);
    }

    /** 모델이 부른 도구 하나. {@code arguments} 는 JSON 문자열 그대로다. */
    public record ToolCall(String id, String name, String arguments) {
    }

    public static LlmMessage system(String content) {
        return new LlmMessage("system", content);
    }

    public static LlmMessage user(String content) {
        return new LlmMessage("user", content);
    }

    public static LlmMessage assistant(String content) {
        return new LlmMessage("assistant", content);
    }

    /** 도구를 부른 assistant 차례. 글은 비어 있을 수 있다. */
    public static LlmMessage assistantToolCalls(String content, List<ToolCall> calls) {
        return new LlmMessage("assistant", content == null ? "" : content, List.copyOf(calls), null);
    }

    /** 도구 결과. */
    public static LlmMessage toolResult(String toolCallId, String content) {
        return new LlmMessage("tool", content, null, toolCallId);
    }
}
