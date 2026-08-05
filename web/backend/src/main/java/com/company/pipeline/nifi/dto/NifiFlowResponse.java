package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * GET /nifi-api/flow/process-groups/{id} 응답 중 잡 카탈로그에 필요한 부분만.
 *
 * <p>{@link NifiFlowStatusResponse}(상태 스냅샷)와 달리 이쪽은 "구성"이다 - 프로세서
 * 설정값, 연결의 관계 이름, 캔버스 좌표가 들어 있다. 한 번 호출하면 그 그룹 한 단계만
 * 내려오므로 하위 그룹은 재귀로 다시 부른다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiFlowResponse(ProcessGroupFlowEntity processGroupFlow) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessGroupFlowEntity(String id, String parentGroupId,
                                         ParameterContextReference parameterContext, Flow flow) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ParameterContextReference(String id, ParameterContextComponent component) {

        public String name() {
            return component == null ? null : component.name();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ParameterContextComponent(String name) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Flow(List<ProcessGroupEntity> processGroups,
                       List<ProcessorEntity> processors,
                       List<ConnectionEntity> connections) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessGroupEntity(String id, ProcessGroupComponent component) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessGroupComponent(String id, String name, String comments, Position position,
                                        Integer runningCount, Integer stoppedCount,
                                        Integer invalidCount, Integer disabledCount,
                                        ParameterContextReference parameterContext) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessorEntity(String id, ProcessorComponent component) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessorComponent(String id, String name, String type, Position position,
                                     String state, String validationStatus, ProcessorConfig config) {

        /** org.apache.nifi.processors.standard.ExecuteSQL -> ExecuteSQL */
        public String shortType() {
            if (type == null) {
                return null;
            }
            int lastDot = type.lastIndexOf('.');
            return lastDot < 0 ? type : type.substring(lastDot + 1);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProcessorConfig(Map<String, String> properties, String schedulingStrategy,
                                  String schedulingPeriod) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConnectionEntity(String id, ConnectionComponent component) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConnectionComponent(String id, ConnectionEndpoint source,
                                      ConnectionEndpoint destination,
                                      List<String> selectedRelationships) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConnectionEndpoint(String id, String name, String type) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Position(Double x, Double y) {
    }
}
