package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** GET /nifi-api/flow/process-groups/root/status?recursive=true 응답 중 필요한 필드만. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiFlowStatusResponse(ProcessGroupStatus processGroupStatus) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessGroupStatus(AggregateSnapshot aggregateSnapshot) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AggregateSnapshot(
            List<ProcessorStatusEntry> processorStatusSnapshots,
            List<ProcessGroupStatusEntry> processGroupStatusSnapshots
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessorStatusEntry(ProcessorStatus processorStatusSnapshot) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessorStatus(String id, String name, String type) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessGroupStatusEntry(ProcessGroupStatusSnapshot processGroupStatusSnapshot) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessGroupStatusSnapshot(
            String id,
            String name,
            List<ProcessorStatusEntry> processorStatusSnapshots,
            List<ProcessGroupStatusEntry> processGroupStatusSnapshots
    ) {
    }
}
