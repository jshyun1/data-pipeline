package com.company.pipeline.llm.chat;

import com.company.pipeline.llm.LlmDeployment;
import java.util.List;

/**
 * [보내는 정보 보기] 응답 - 실제로 전송될 대상 정보를 <b>마스킹 · 자르기가 적용된 모습 그대로</b> 보여 준다(설계 5-2).
 * 역할 지시문은 넣지 않는다(대상 정보가 아니고, 모든 요청에 같다).
 */
public record LlmContextPreview(
        String targetLabel,
        String providerName,
        LlmDeployment deployment,
        String modelName,
        boolean masked,
        boolean truncated,
        List<String> omittedSections,
        int inputChars,
        int maxInputChars,
        String text) {
}
