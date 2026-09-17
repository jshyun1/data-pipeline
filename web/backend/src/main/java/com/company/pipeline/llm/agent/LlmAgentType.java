package com.company.pipeline.llm.agent;

/** 분석 유형(설계 5-4). 유형마다 역할 지시문과 답변 형식이 다르다. */
public enum LlmAgentType {
    /** 로그 화면의 [GPT 분석] - 오류 원인과 조치. */
    ERROR_ANALYSIS,
    /** 속성창 GPT 탭 - 대상에 대한 자유 질문(3단계). */
    JOB_ANALYSIS
}
