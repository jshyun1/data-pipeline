package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SensitiveDataMaskerTest {

    @Test
    void masksKindButKeepsStructure() {
        String original = "VALUES ('홍길동', '010-1234-5678', 'hong@company.com', '900101-1234567') was aborted: "
                + "duplicate key value violates unique constraint \"customer_pkey\"";

        assertThat(SensitiveDataMasker.mask(original)).isEqualTo(
                "VALUES ('홍길동', '<전화번호>', '<이메일>', '<주민번호>') was aborted: "
                        + "duplicate key value violates unique constraint \"customer_pkey\"");
    }

    @Test
    void cardNumberNeedsLuhn() {
        assertThat(SensitiveDataMasker.mask("card 4111-1111-1111-1111")).isEqualTo("card <카드번호>");
        // Luhn 을 통과하지 않는 16자리 숫자(식별자 등)는 그대로 둔다
        assertThat(SensitiveDataMasker.mask("id 1234567812345678")).isEqualTo("id 1234567812345678");
    }

    @Test
    void longNumbersThatAreNotPersonalDataStayIntact() {
        // Kafka offset · epoch 밀리초 · 주민번호처럼 보이지만 날짜가 아닌 숫자
        String text = "offset=1757990012345 lag=0 ts=1757990000000 rows=1660590 run=manual__2026-09-16T01:30:00+00:00";
        assertThat(SensitiveDataMasker.mask(text)).isEqualTo(text);
    }

    @Test
    void phoneVariants() {
        assertThat(SensitiveDataMasker.mask("01012345678 / 02-123-4567 / 031.1234.5678"))
                .isEqualTo("<전화번호> / <전화번호> / <전화번호>");
    }

    @Test
    void nullAndEmptyPassThrough() {
        assertThat(SensitiveDataMasker.mask(null)).isNull();
        assertThat(SensitiveDataMasker.mask("")).isEmpty();
    }
}
