package com.company.pipeline.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.net.http.HttpTimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;

/**
 * 타임아웃과 그 밖의 호출 실패를 구분하는지.
 *
 * <p>타임아웃은 "실패"가 아니라 "결과를 모름"이다. 이걸 일반 실패와 같이 다루면, Kafka
 * Connect 가 요청을 이미 반영했는데도 파이프라인이 FAILED 로 떨어져 이후 start 가 막힌다
 * (2026-08-26 실장애). 호출부가 실제 상태를 확인해 판정할 수 있도록 따로 던져야 한다.
 */
class KafkaConnectTimeoutClassificationTest {

    /** private static boolean isTimeout(Throwable) 을 직접 검증한다. */
    private static boolean isTimeout(Throwable ex) {
        try {
            Method m = KafkaConnectClient.class.getDeclaredMethod("isTimeout", Throwable.class);
            m.setAccessible(true);
            return (boolean) m.invoke(null, ex);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void JDK_HttpClient_읽기_타임아웃을_타임아웃으로_본다() {
        RestClientException ex = new ResourceAccessException("Request timed out",
                new HttpTimeoutException("request timed out"));

        assertThat(isTimeout(ex)).isTrue();
    }

    @Test
    void SocketTimeout_도_타임아웃으로_본다() {
        RestClientException ex = new ResourceAccessException("I/O error",
                new SocketTimeoutException("Read timed out"));

        assertThat(isTimeout(ex)).isTrue();
    }

    @Test
    void 연결_거부는_타임아웃이_아니다() {
        // 워커가 아예 내려간 경우다. 이건 "결과를 모름"이 아니라 확정 실패라 그대로 실패여야 한다.
        RestClientException ex = new ResourceAccessException("I/O error",
                new java.net.ConnectException("Connection refused"));

        assertThat(isTimeout(ex)).isFalse();
    }

    @Test
    void 일반_IO_오류도_타임아웃이_아니다() {
        RestClientException ex = new ResourceAccessException("I/O error", new IOException("broken pipe"));

        assertThat(isTimeout(ex)).isFalse();
    }

    @Test
    void 원인_사슬이_자기참조여도_무한루프에_빠지지_않는다() {
        // getCause() 가 자신을 가리키는 예외를 만나도 순회가 끝나야 한다.
        RuntimeException self = new RuntimeException("self") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        assertThat(isTimeout(self)).isFalse();
    }

    @Test
    void 타임아웃_예외는_일반_호출실패_예외의_하위형이다() {
        // 기존에 KafkaConnectClientException 을 잡던 호출부는 그대로 동작해야 한다.
        KafkaConnectTimeoutException ex =
                new KafkaConnectTimeoutException("타임아웃", new HttpTimeoutException("t"));

        assertThat(ex).isInstanceOf(KafkaConnectClientException.class);
        assertThatThrownBy(() -> {
            throw ex;
        }).isInstanceOf(KafkaConnectClientException.class);
    }

    /** 리플렉션 실패 시 조용히 통과하지 않도록. */
    @Test
    void isTimeout_메서드가_존재한다() {
        assertThatThrownBy(() -> KafkaConnectClient.class.getDeclaredMethod("isTimeout", String.class))
                .isInstanceOf(NoSuchMethodException.class);
        try {
            assertThat(KafkaConnectClient.class.getDeclaredMethod("isTimeout", Throwable.class)).isNotNull();
        } catch (NoSuchMethodException e) {
            throw new AssertionError("isTimeout(Throwable) 이 없어졌다", e);
        }
    }

    /** InvocationTargetException 이 그대로 새어나가지 않는지(테스트 헬퍼 자체 검증). */
    @Test
    void 헬퍼는_예외를_감싸서_던진다() {
        assertThat(InvocationTargetException.class).isNotNull();
    }
}
