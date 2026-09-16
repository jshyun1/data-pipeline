package com.company.pipeline.llm;

/** LLM 이 어디서 도는가. API 키 필수 여부를 정한다(외부는 항상 필수). */
public enum LlmDeployment {
    /** Claude·ChatGPT·Gemini 같은 외부 제공자. */
    EXTERNAL,
    /** 폐쇄망에 반입해 내부에서 서빙하는 오픈 LLM(Ollama·vLLM·LM Studio 등). */
    INTERNAL
}
