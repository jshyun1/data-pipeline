package com.company.pipeline.monitoring;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/** Kafka Connect가 아닌 Kafka Broker 자체의 가용성을 describeCluster()로 확인한다. */
@Component
@EnableConfigurationProperties(KafkaBrokerProperties.class)
public class KafkaBrokerHealthChecker {

    private final AdminClient adminClient;

    public KafkaBrokerHealthChecker(KafkaBrokerProperties properties) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        this.adminClient = AdminClient.create(config);
    }

    public boolean isHealthy() {
        try {
            adminClient.describeCluster().nodes().get(3, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException ex) {
            return false;
        }
    }

    @PreDestroy
    public void close() {
        adminClient.close(Duration.ofSeconds(3));
    }
}
