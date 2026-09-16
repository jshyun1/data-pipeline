package com.company.pipeline.llm.dto;

import com.company.pipeline.llm.LlmDeployment;
import com.company.pipeline.llm.LlmProvider;

/**
 * GPT 기능이 켜져 있는지. 속성창 GPT 탭·오류 분석 버튼이 이걸 보고 <b>그리기 자체를</b> 결정한다.
 * 로그인 사용자 누구나 부르므로 키·주소는 넣지 않는다.
 */
public record LlmStatusResponse(
        boolean enabled,
        String providerName,
        LlmDeployment deployment,
        LlmProvider provider,
        String modelName,
        Boolean maskSensitive) {

    public static LlmStatusResponse disabled() {
        return new LlmStatusResponse(false, null, null, null, null, null);
    }
}
