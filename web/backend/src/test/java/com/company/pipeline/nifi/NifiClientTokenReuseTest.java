package com.company.pipeline.nifi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 실제 HTTP 로 NifiClient 의 토큰 재사용 · 401 재시도를 확인한다(2026-10-06).
 * 가짜 NiFi 는 발급 횟수를 세고, 무효로 표시한 토큰에는 401 을 준다.
 */
class NifiClientTokenReuseTest {

    private HttpServer server;
    private final AtomicInteger issued = new AtomicInteger();
    private volatile boolean rejectAll;
    private NifiClient client;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/nifi-api/access/token", exchange -> {
            String token = NifiTokenCacheTest.jwt(Instant.now().plus(Duration.ofHours(8)), "t" + issued.incrementAndGet());
            respond(exchange, 201, token);
        });
        server.createContext("/nifi-api/flow/bulletin-board", exchange -> {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            boolean ok = auth != null && auth.startsWith("Bearer ") && !rejectAll;
            respond(exchange, ok ? 200 : 401, ok ? "{}" : "Unauthorized");
        });
        server.start();
        client = new NifiClient(new NifiProperties("http://127.0.0.1:" + server.getAddress().getPort(), "svc", "pw"));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", status == 200 ? "application/json" : "text/plain");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @Test
    void 여러_번_불러도_토큰은_한_번만_받는다() {
        for (int i = 0; i < 5; i++) {
            client.getBulletins(0L);
        }
        assertThat(issued).hasValue(1);
    }

    @Test
    void 토큰이_거절되면_새로_받아_한_번만_다시_보낸다() {
        client.getBulletins(0L);
        rejectFirstIssued();

        client.getBulletins(0L);
        assertThat(issued).hasValue(2);

        client.getBulletins(0L);
        assertThat(issued).hasValue(2);
    }

    @Test
    void 새_토큰도_거절되면_한_번만_다시_받고_오류로_끝낸다() {
        client.getBulletins(0L);
        rejectAll = true;

        assertThatThrownBy(() -> client.getBulletins(0L)).isInstanceOf(NifiClientException.class);
        assertThat(issued).hasValue(2);
    }

    /** 첫 토큰을 무효로 만든다 - 발급한 토큰 문자열을 서버가 기억하지 않으므로 «t1» 표시로 찾는다. */
    private void rejectFirstIssued() {
        server.removeContext("/nifi-api/flow/bulletin-board");
        server.createContext("/nifi-api/flow/bulletin-board", exchange -> {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            boolean first = auth != null && decodedPayload(auth.substring(7)).contains("\"t1\"");
            respond(exchange, first ? 401 : 200, first ? "Unauthorized" : "{}");
        });
    }

    private static String decodedPayload(String jwt) {
        String[] parts = jwt.split("\\.");
        return parts.length < 2 ? "" : new String(java.util.Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
    }
}
