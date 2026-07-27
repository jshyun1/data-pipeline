package com.company.pipeline.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaConnectClientTest {

    private MockWebServer server;
    private KafkaConnectClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        client = new KafkaConnectClient(new KafkaConnectProperties(server.url("/").toString()));
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    void listConnectors_parsesJsonArray() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("[\"oracle-cdc-source\",\"postgres-cdc-sink\"]")
                .addHeader("Content-Type", "application/json"));

        List<String> connectors = client.listConnectors();

        assertThat(connectors).containsExactly("oracle-cdc-source", "postgres-cdc-sink");
        assertThat(server.takeRequest().getPath()).isEqualTo("/connectors");
    }

    @Test
    void getStatus_parsesConnectorAndTaskState() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("""
                        {"name":"oracle-cdc-source","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},
                         "tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"source"}
                        """)
                .addHeader("Content-Type", "application/json"));

        ConnectorStatusResponse status = client.getStatus("oracle-cdc-source");

        assertThat(status.connector().state()).isEqualTo("RUNNING");
        assertThat(status.tasks()).hasSize(1);
        assertThat(status.tasks().get(0).state()).isEqualTo("RUNNING");
    }

    @Test
    void upsertConfig_sendsPutWithBody() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(201)
                .setBody("""
                        {"name":"source-1-oracle-appuser-customers","config":{},"tasks":[]}
                        """)
                .addHeader("Content-Type", "application/json"));

        client.upsertConfig("source-1-oracle-appuser-customers", Map.of("connector.class", "x"));

        var recorded = server.takeRequest();
        assertThat(recorded.getMethod()).isEqualTo("PUT");
        assertThat(recorded.getPath()).isEqualTo("/connectors/source-1-oracle-appuser-customers/config");
    }

    @Test
    void createStopped_postsInitialStoppedState() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(201)
                .setBody("""
                        {"name":"source-1","config":{},"tasks":[]}
                        """)
                .addHeader("Content-Type", "application/json"));

        client.createStopped("source-1", Map.of("connector.class", "x"));

        var recorded = server.takeRequest();
        assertThat(recorded.getMethod()).isEqualTo("POST");
        assertThat(recorded.getPath()).isEqualTo("/connectors");
        assertThat(recorded.getBody().readUtf8())
                .contains("\"name\":\"source-1\"")
                .contains("\"initial_state\":\"STOPPED\"")
                .contains("\"connector.class\":\"x\"");
    }

    @Test
    void stop_sendsKafkaConnectStopRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));

        client.stop("sink-1");

        var recorded = server.takeRequest();
        assertThat(recorded.getMethod()).isEqualTo("PUT");
        assertThat(recorded.getPath()).isEqualTo("/connectors/sink-1/stop");
    }

    @Test
    void kafkaConnectError_wrapsIntoKafkaConnectClientException() {
        server.enqueue(new MockResponse()
                .setResponseCode(500)
                .setBody("{\"error_code\":500,\"message\":\"boom\"}")
                .addHeader("Content-Type", "application/json"));

        assertThatThrownBy(() -> client.listConnectors())
                .isInstanceOf(KafkaConnectClientException.class);
    }
}
