package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * GET /nifi-api/processors/{id} 응답 중 필요한 부분만.
 *
 * <p>status(recursive) 응답에는 프로세서 "설정"이 없어서, 적재 대상 테이블 같은 값은
 * 이 엔드포인트로 따로 읽어야 한다. 프로세서마다 한 번만 읽고 캐시한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiProcessorDetailResponse(String id, Revision revision, Permissions permissions,
                                          Component component, Status status,
                                          List<BulletinEntity> bulletins) {

    public NifiProcessorDetailResponse(Component component) {
        this(null, null, null, component, null, List.of());
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Revision(Long version) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Permissions(Boolean canRead, Boolean canWrite) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Component(String id, String parentGroupId, String name, String type, String bundleGroup,
                            String bundleArtifact, String bundleVersion, Bundle bundle, String state,
                            String validationStatus, Position position, String comments, Config config,
                            Map<String, PropertyDescriptor> descriptors,
                            Map<String, PropertyDescriptor> propertyDescriptors,
                            List<Relationship> relationships, List<String> autoTerminatedRelationships,
                            String supportsParallelProcessing, String supportsEventDriven,
                            String supportsBatching) {

        public Component(String id, String name, String type, Config config) {
            this(id, null, name, type, null, null, null, null, null, null, null, null, config,
                    null, null, List.of(), List.of(), null, null, null);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Bundle(String group, String artifact, String version) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Position(Double x, Double y) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Config(Map<String, String> properties, Map<String, PropertyDescriptor> descriptors,
                         String schedulingStrategy, String schedulingPeriod, String executionNode,
                         String penaltyDuration, String yieldDuration, Integer concurrentlySchedulableTaskCount,
                         String comments, String runDurationMillis, String bulletinLevel,
                         String retryCount, String retriedRelationships,
                         List<String> autoTerminatedRelationships) {

        public Config(Map<String, String> properties) {
            this(properties, null, null, null, null, null, null, null, null, null, null, null, null, List.of());
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PropertyDescriptor(String name, String displayName, String description, Boolean sensitive,
                                     Boolean dynamic, Boolean required,
                                     Boolean identifiesControllerService) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Relationship(String name, String description, Boolean autoTerminate) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Status(String runStatus, String validationStatus, Integer activeThreadCount,
                         AggregateSnapshot aggregateSnapshot) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AggregateSnapshot(String runStatus, String validationStatus, Integer activeThreadCount) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BulletinEntity(Bulletin bulletin) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Bulletin(String level, String category, String message, String timestamp) {
    }

    /** PutDatabaseRecord의 적재 대상을 "schema.table"로. 알 수 없으면 null. */
    public String targetTable() {
        if (component == null || component.config() == null || component.config().properties() == null) {
            return null;
        }
        Map<String, String> properties = component.config().properties();
        String table = properties.get("put-db-record-table-name");
        if (table == null || table.isBlank()) {
            return null;
        }
        String schema = properties.get("put-db-record-schema-name");
        return schema == null || schema.isBlank() ? table : schema + "." + table;
    }
}
