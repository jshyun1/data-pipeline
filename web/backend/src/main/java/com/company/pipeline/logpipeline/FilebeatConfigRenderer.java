package com.company.pipeline.logpipeline;

import org.springframework.stereotype.Component;

/**
 * LogPipelineSource -> Filebeat filestream input YAML 텍스트로 렌더링하는 순수 함수
 * (DebeziumOracleTemplate/JdbcSinkTemplate과 같은 "렌더링만, I/O 없음" 원칙).
 *
 * Kafka Connect worker의 value converter 기본값(schemas.enable=true)을 안 건드리기 위해,
 * 여기서 만드는 각 이벤트는 script 프로세서로 Kafka Connect의 {"schema":..,"payload":..}
 * 봉투를 직접 만들어서 하나의 문자열 필드(kafka_value_json)에 담고, output.kafka의
 * codec.format.string이 그 필드 값을 그대로 메시지 바디로 보낸다 - JSON.stringify를 쓰는
 * 이유는 로그 라인에 따옴표/개행이 섞여도 안전하게 이스케이프되기 때문
 * (수작업 문자열 템플릿은 이 경우 깨진다 - PipelineDeployServiceTest 등과 별개로
 * 이 부분은 실제 Filebeat 컨테이너로 직접 검증해야 하는 영역, 스파이크는 수신 측만 검증함).
 *
 * 토픽 라우팅은 fields.kafka_topic 기반 output.kafka.topic 템플릿으로 처리하고, 이 필드는
 * 봉투 문자열 안에는 안 들어간다 - JsonConverter가 schema/payload 외 다른 최상위 키가
 * 있으면 예외를 던지기 때문에 봉투는 반드시 이 2개 키만 가져야 한다.
 */
@Component
public class FilebeatConfigRenderer {

    public String render(Long pipelineId, LogPipelineSource source) {
        String id = "pipeline-" + pipelineId;
        String path = resolvePath(source);
        boolean tailFromEnd = !"BEGINNING".equalsIgnoreCase(source.getReadFrom());
        String agentHost = source.getAgentHost() != null ? source.getAgentHost() : "filebeat";

        return """
                - type: filestream
                  id: %s
                  paths:
                    - "%s"
                  encoding: %s
                  tail_files: %s
                  fields:
                    kafka_topic: "%s"
                  fields_under_root: false
                  processors:
                    - script:
                        lang: javascript
                        source: >
                          function process(event) {
                            var schema = {
                              type: "struct",
                              fields: [
                                {field: "message", type: "string", optional: true},
                                {field: "log_timestamp", type: "string", optional: true},
                                {field: "source_file", type: "string", optional: true},
                                {field: "agent_host", type: "string", optional: true}
                              ],
                              optional: false,
                              name: "logline"
                            };
                            var payload = {
                              message: event.Get("message"),
                              log_timestamp: new Date().toISOString(),
                              source_file: event.Get("log.file.path"),
                              agent_host: "%s"
                            };
                            event.Put("kafka_value_json", JSON.stringify({schema: schema, payload: payload}));
                          }
                """.formatted(id, path, source.getEncoding().toLowerCase(), tailFromEnd,
                source.getTopicName(), agentHost);
    }

    private String resolvePath(LogPipelineSource source) {
        String filePath = source.getFilePath();
        String pattern = source.getFilePattern();
        if (pattern == null || pattern.isBlank()) {
            return filePath;
        }
        String base = filePath.endsWith("/") ? filePath : filePath + "/";
        return base + pattern;
    }
}
