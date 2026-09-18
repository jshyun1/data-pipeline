package com.company.pipeline.llm.context;

import java.util.Map;

/**
 * 대상 종류 하나의 «기본 정보»를 서버에서 모은다(설계 5-2). 화면은 대상 식별자와 화면에 보이는 오류 문구만 보내고,
 * 무엇을 보낼지는 여기서 정한다 - 보내면 안 되는 값(연결 계정 · 비밀번호 · 키)이 섞일 위험을 화면마다 떠안지 않게.
 *
 * <p>구현체는 스프링 빈으로 두면 {@code LlmChatService} 가 {@link #targetType()} 으로 찾아 쓴다. ETL 쪽 수집기도 같은 방식으로 붙인다.
 */
public interface LlmContextCollector {

    LlmTargetType targetType();

    /**
     * @param target    화면이 보낸 대상 식별자(대상 종류마다 키가 다르다)
     * @param errorText 화면에 보이는 오류 문구. 속성창 질문처럼 없을 수도 있다
     * @throws com.company.pipeline.common.BusinessException 대상이 없거나 식별자가 모자랄 때
     */
    CollectedTarget collect(Map<String, String> target, String errorText);

    /**
     * 화면이 작성 중 설계(draft)를 같이 보내는 대상(WORKFLOW_DRAFT)용. 나머지 수집기는 draft 를 쓰지 않는다.
     */
    default CollectedTarget collect(Map<String, String> target, String errorText, String draft) {
        return collect(target, errorText);
    }

    /**
     * @param targetId 호출 기록에 남길 식별자(짧게)
     * @param label    화면에 보여 줄 대상 이름
     */
    record CollectedTarget(String targetId, String label, LlmContext context) {
    }
}
