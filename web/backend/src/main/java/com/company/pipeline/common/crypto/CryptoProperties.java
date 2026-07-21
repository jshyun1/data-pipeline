package com.company.pipeline.common.crypto;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pipeline.crypto")
public record CryptoProperties(String secret, String salt) {
}
