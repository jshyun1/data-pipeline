package com.company.pipeline.logpipeline;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** FILEBEAT_INPUTS_DIR 환경변수로 설정 (docker-compose 네트워크 안에서는 /filebeat-inputs, filebeat 컨테이너와 공유 볼륨). */
@ConfigurationProperties(prefix = "filebeat")
public record FilebeatProperties(String inputsDir) {
}
