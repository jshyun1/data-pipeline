package com.company.pipeline.llm;

/** 과금 구분. 표시·관리용이며 API 키 필수 여부와는 무관하다(그건 {@link LlmDeployment} 가 정한다). */
public enum LlmBillingType {
    FREE,
    PAID
}
