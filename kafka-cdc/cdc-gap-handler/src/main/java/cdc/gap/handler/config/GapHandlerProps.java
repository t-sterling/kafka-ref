package cdc.gap.handler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed access to application.yml values under app.*
 */
@ConfigurationProperties(prefix = "gap-handler")
public record GapHandlerProps(Topics topics, State state) {

    public record Topics(
        String cdcIn,
        String cdcOut,
        String cdcFillCommand,
        String cdcFillEvent
    ) {}

    public record State(String storeName, int maxBufferPerRecord) {}

}

