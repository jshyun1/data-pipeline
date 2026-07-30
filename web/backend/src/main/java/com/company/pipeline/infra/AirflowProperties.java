package com.company.pipeline.infra;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Airflow api-server 주소. docker-compose 네트워크 안에서는 이 기본값이 항상 맞으므로,
 * 오래된 .env로 배포해도 인프라 구역이 깨지지 않도록 기본값을 코드에도 둔다.
 */
@ConfigurationProperties(prefix = "airflow")
public record AirflowProperties(String baseUrl) {

    public AirflowProperties {
        baseUrl = StringUtils.hasText(baseUrl) ? baseUrl : "http://airflow-apiserver:8080";
    }
}
