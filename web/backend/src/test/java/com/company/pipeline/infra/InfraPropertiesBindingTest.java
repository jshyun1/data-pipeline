package com.company.pipeline.infra;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * 인프라 구역 설정이 환경변수로 실제로 묶이는지. 여기서 어긋나면 대시보드 한 구역이
 * 아니라 애플리케이션 기동 자체가 실패하므로 컨텍스트 수준에서 확인한다.
 */
class InfraPropertiesBindingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(TestConfig.class);

    @Test
    void bindsDiskPathsFromCommaSeparatedValue() {
        runner.withPropertyValues("infra.proc-path=/host-proc", "infra.disk-paths=/,/var/lib/docker")
                .run(context -> {
                    InfraProperties properties = context.getBean(InfraProperties.class);
                    assertThat(properties.procPath()).isEqualTo("/host-proc");
                    assertThat(properties.diskPaths()).containsExactly("/", "/var/lib/docker");
                });
    }

    @Test
    void usesDefaultsWhenPropertiesAreAbsent() {
        runner.run(context -> {
            assertThat(context.getBean(InfraProperties.class).procPath()).isEqualTo("/proc");
            assertThat(context.getBean(InfraProperties.class).diskPaths()).containsExactly("/");
            assertThat(context.getBean(AirflowProperties.class).baseUrl())
                    .isEqualTo("http://airflow-apiserver:8080");
        });
    }

    @Test
    void emptyEnvironmentValueFallsBackToDefault() {
        // 오래된 .env로 배포해 값이 빈 문자열로 들어오는 경우.
        runner.withPropertyValues("airflow.base-url=", "infra.proc-path=").run(context -> {
            assertThat(context.getBean(AirflowProperties.class).baseUrl())
                    .isEqualTo("http://airflow-apiserver:8080");
            assertThat(context.getBean(InfraProperties.class).procPath()).isEqualTo("/proc");
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({ InfraProperties.class, AirflowProperties.class })
    static class TestConfig {
    }
}
