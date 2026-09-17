package com.company.pipeline.llm.client;

/**
 * LLM 호출 실패. {@code code} 는 연결 테스트와 같은 분류(AUTH_FAILED · NOT_FOUND · RATE_LIMITED · TIMEOUT · UNREACHABLE …)라
 * 화면이 같은 문구 규칙으로 안내할 수 있다. 메시지에는 키를 절대 넣지 않는다.
 */
public class LlmCallException extends RuntimeException {

    private final String code;
    private final Integer httpStatus;

    public LlmCallException(String code, String message, Integer httpStatus) {
        super(message);
        this.code = code;
        this.httpStatus = httpStatus;
    }

    public String getCode() {
        return code;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }
}
