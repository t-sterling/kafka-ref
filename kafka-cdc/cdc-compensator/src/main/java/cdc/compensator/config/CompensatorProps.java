package cdc.compensator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed access to application.yml values under app.*
 */
@ConfigurationProperties(prefix = "compensator")
public record CompensatorProps(Topics topics, String baseUrl) {

    public record Topics(
        String cdcFillCommand,
        String cdcFillEvent
    ) {}

}


