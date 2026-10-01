package com.company.pipeline.llm.tool;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 질문 속 이름 맞추기(2026-10-02). 질문은 이미 소문자로 넘어온다. */
class RelatedTargetFinderTest {

    @Test
    void wholeNameOrLongEnoughPiece_matches() {
        String q = "tb_imp018m은 어떤 방식으로 구성된 job이야?";
        assertThat(RelatedTargetFinder.mentioned(q, "AAA-tb_imp018m")).isTrue();
        assertThat(RelatedTargetFinder.mentioned(q, "TB_IMP018M")).isTrue();
        assertThat(RelatedTargetFinder.mentioned("dz_com001m 단계 알려줘", "DZ_COM001M")).isTrue();
    }

    @Test
    void shortPiecesAndUnrelatedNames_doNotMatch() {
        String q = "aaa 는 뭐야?";
        assertThat(RelatedTargetFinder.mentioned(q, "AAA-tb_imp060l")).isFalse();
        assertThat(RelatedTargetFinder.mentioned("tb_imp018m 구성", "AAA-tb_imp060l")).isFalse();
        assertThat(RelatedTargetFinder.mentioned("아무 질문", (String) null)).isFalse();
    }
}
