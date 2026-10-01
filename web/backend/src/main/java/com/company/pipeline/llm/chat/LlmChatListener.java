package com.company.pipeline.llm.chat;

/** 스트리밍 진행을 화면 쪽(SSE)으로 내보내는 창구. 구현체가 쓰기에 실패하면 예외를 던져 호출을 멈춘다. */
public interface LlmChatListener {

    void meta(LlmContextPreview preview);

    /**
     * 대화가 길어 앞부분을 줄였다(답이 흐르기 전에 한 번).
     *
     * @param mode         DROPPED(오래된 문답을 뺌) · SUMMARIZED(요약으로 바꿈)
     * @param droppedTurns 화면이 보낸 대화 목록 앞에서부터 줄인 개수
     * @param droppedPairs 줄인 문답(질문 + 답) 수 - 안내 문구용
     * @param summary      SUMMARIZED 면 새 요약. 화면이 다음 요청부터 이것을 보낸다
     */
    void compacted(String mode, int droppedTurns, int droppedPairs, String summary);

    void delta(String text);

    void done(Integer inputTokens, Integer outputTokens, long durationMs);

    void error(String code, String message);

    /** 도구 호출을 이번 답에 쓰는지(2026-10-02). 화면이 답 위 정보 줄에 «도구 조회 사용 · 미사용»을 적는다. */
    default void toolMode(boolean enabled) {
    }

    /** 모델이 도구 하나를 불러 실행했다. {@code label} 은 «CDC 파이프라인 «AAA-tb_imp018m» 조회» · ««dz» 검색» 같은 사람 말이다. */
    default void tool(String name, String label) {
    }
}
