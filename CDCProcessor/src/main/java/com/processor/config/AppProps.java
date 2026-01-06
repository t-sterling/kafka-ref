package com.processor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed access to application.yml values under app.*
 */
@ConfigurationProperties(prefix = "app")
public record AppProps(Topics topics, Normalizer normalizer) {

    public record Topics(String input, String output, String recovery, String refreshRequest) {}

    public record Normalizer(String storeName, int maxBufferPerRecord) {}

}

