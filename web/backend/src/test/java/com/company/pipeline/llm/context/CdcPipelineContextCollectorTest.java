package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CdcPipelineContextCollectorTest {

    @Test
    void summarizeTrace_keepsHeadAndCausedByOnly() {
        StringBuilder trace = new StringBuilder("org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask\n");
        for (int i = 0; i < 200; i++) {
            trace.append("\tat org.apache.kafka.Frame").append(i).append("(Frame.java:1)\n");
        }
        trace.append("Caused by: java.sql.BatchUpdateException: value too long for type character varying(10)\n");
        trace.append("\tat org.postgresql.Frame(Frame.java:1)\n");

        String summary = CdcPipelineContextCollector.summarizeTrace(trace.toString());

        assertThat(summary).startsWith("org.apache.kafka.connect.errors.ConnectException")
                .contains("Caused by: java.sql.BatchUpdateException: value too long")
                .doesNotContain("Frame199");
        assertThat(summary.lines().count()).isLessThan(20);
    }
}
