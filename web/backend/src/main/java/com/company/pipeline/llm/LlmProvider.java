package com.company.pipeline.llm;

/**
 * LLM 제공자. 호출 형식(요청·인증 헤더)이 제공자마다 달라 연결 테스트와 이후 호출이 이 값으로 갈린다.
 *
 * <p>내부 오픈 LLM 은 모델마다 따로 만들지 않는다. Ollama·vLLM·LM Studio 등은 대부분 OpenAI 와 같은
 * 형식의 API 를 제공하므로 {@link #OPENAI_COMPATIBLE} 하나에 주소만 바꿔 넣으면 된다.
 */
public enum LlmProvider {
    ANTHROPIC(LlmDeployment.EXTERNAL, "https://api.anthropic.com", 100_000),
    OPENAI(LlmDeployment.EXTERNAL, "https://api.openai.com/v1", 100_000),
    GEMINI(LlmDeployment.EXTERNAL, "https://generativelanguage.googleapis.com", 100_000),
    /** 내부 서빙은 입력 길이가 짧은 모델이 많아 기본값을 작게 둔다. 등록 화면에서 바꿀 수 있다. */
    OPENAI_COMPATIBLE(LlmDeployment.INTERNAL, null, 16_000);

    private final LlmDeployment deployment;
    private final String defaultBaseUrl;
    private final int defaultMaxInputChars;

    LlmProvider(LlmDeployment deployment, String defaultBaseUrl, int defaultMaxInputChars) {
        this.deployment = deployment;
        this.defaultBaseUrl = defaultBaseUrl;
        this.defaultMaxInputChars = defaultMaxInputChars;
    }

    public LlmDeployment getDeployment() {
        return deployment;
    }

    /** 외부 제공자의 기본 API 주소. 내부는 주소가 현장마다 달라 null. */
    public String getDefaultBaseUrl() {
        return defaultBaseUrl;
    }

    public int getDefaultMaxInputChars() {
        return defaultMaxInputChars;
    }
}
