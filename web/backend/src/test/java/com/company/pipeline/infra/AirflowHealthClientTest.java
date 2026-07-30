package com.company.pipeline.infra;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AirflowHealthClientTest {

    private MockWebServer server;
    private AirflowHealthClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        client = new AirflowHealthClient(new AirflowProperties(server.url("/").toString()));
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    /** 실제 airflow-apiserver(3.2.2) 응답 그대로 - dag_processor 같은 snake_case 키를 놓치지 않는지. */
    @Test
    void getHealth_parsesLiveResponseShape() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("""
                        {"metadatabase":{"status":"healthy"},
                         "scheduler":{"status":"healthy","latest_scheduler_heartbeat":"2026-07-30T04:18:17.557964+00:00"},
                         "triggerer":{"status":null,"latest_triggerer_heartbeat":null},
                         "dag_processor":{"status":"healthy","latest_dag_processor_heartbeat":"2026-07-30T04:18:21.113034+00:00"}}
                        """)
                .addHeader("Content-Type", "application/json"));

        var health = client.getHealth().orElseThrow();

        assertThat(health.metadatabase().status()).isEqualTo("healthy");
        assertThat(health.scheduler().status()).isEqualTo("healthy");
        assertThat(health.scheduler().latestHeartbeat()).isEqualTo("2026-07-30T04:18:17.557964+00:00");
        assertThat(health.dagProcessor().status()).isEqualTo("healthy");
        assertThat(health.dagProcessor().latestHeartbeat()).isEqualTo("2026-07-30T04:18:21.113034+00:00");
        // 이 배포에는 triggerer 컨테이너가 없어 status가 null로 온다 - 죽은 것과 구분해야 한다.
        assertThat(health.triggerer().status()).isNull();
        assertThat(server.takeRequest().getPath()).isEqualTo("/api/v2/monitor/health");
    }

    @Test
    void getHealth_serverError_returnsEmptyInsteadOfThrowing() {
        server.enqueue(new MockResponse().setResponseCode(503));

        assertThat(client.getHealth()).isEmpty();
    }

    @Test
    void properties_fillDefaultBaseUrlWhenUnset() {
        assertThat(new AirflowProperties(null).baseUrl()).isEqualTo("http://airflow-apiserver:8080");
    }
}
