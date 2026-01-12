package cdc.compensator.domain;

import java.util.Map;

public record FillEvent(
        String recordId,
        String entityType,
        String eventId,
        long cutoffLastModifiedEpochMs,
        Map<String, Object> allFields
) { }