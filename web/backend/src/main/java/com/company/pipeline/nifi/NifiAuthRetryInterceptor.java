package com.company.pipeline.nifi;

import java.io.IOException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

/**
 * 받아 둔 토큰을 NiFi 가 401 로 거절하면 토큰을 새로 받아 같은 요청을 <b>한 번만</b> 다시 보낸다(2026-10-06, {@link NifiTokenCache}).
 *
 * <p>만료 전에도 NiFi 쪽 사정으로 토큰이 무효가 될 수 있다. 다시 보내도 401 이면 비밀번호가 틀린 것 같은 진짜 오류라
 * 그 응답을 그대로 돌려 예전처럼 오류로 처리되게 한다. 토큰 발급 요청 자체(Bearer 없음)는 건드리지 않는다.
 */
final class NifiAuthRetryInterceptor implements ClientHttpRequestInterceptor {

    private static final String BEARER = "Bearer ";

    private final NifiTokenCache tokens;

    NifiAuthRetryInterceptor(NifiTokenCache tokens) {
        this.tokens = tokens;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {
        ClientHttpResponse response = execution.execute(request, body);
        String authorization = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (response.getStatusCode().value() != 401 || authorization == null || !authorization.startsWith(BEARER)) {
            return response;
        }
        response.close();
        tokens.invalidate(authorization.substring(BEARER.length()));
        request.getHeaders().set(HttpHeaders.AUTHORIZATION, BEARER + tokens.get());
        return execution.execute(request, body);
    }
}
