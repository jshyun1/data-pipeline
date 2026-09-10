package com.company.pipeline.common.config;

import org.flywaydb.core.api.exception.FlywayValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlywayV70RepairStrategy {

    private static final Logger log = LoggerFactory.getLogger(FlywayV70RepairStrategy.class);
    private static final String V70_CHECKSUM_MISMATCH = "Migration checksum mismatch for migration version 70";

    @Bean
    FlywayMigrationStrategy flywayMigrationStrategy() {
        return flyway -> {
            try {
                flyway.migrate();
            } catch (FlywayValidateException exception) {
                if (!isV70ChecksumMismatch(exception)) {
                    throw exception;
                }

                log.warn("V70 checksum mismatch detected. Repairing Flyway history once before applying later migrations.");
                flyway.repair();
                flyway.migrate();
            }
        };
    }

    private boolean isV70ChecksumMismatch(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains(V70_CHECKSUM_MISMATCH)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
