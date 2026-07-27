package com.company.pipeline.nifi;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "nifi")
public record NifiProperties(
        String baseUrl,
        String username,
        String password
) {
}
