package cdc.gap.handler.domain;


import java.util.Map;

/**
 * Produced by a "refresher" service when a full entity reload is complete.
 */
public record FillEvent(
        String recordId,
        String entityType,
        String eventId,
        long cutoffLastModifiedEpochMs,
        Map<String, Object> allFields,
        boolean isSuccess,
        String errMessage
){}