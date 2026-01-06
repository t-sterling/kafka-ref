package com.processor.event;

import java.util.Map;

/**
 * Matches the JSON coming from CDCSource.
 *
 * Notes:
 * - key is recordId at the Kafka level (the message key)
 * - this object also carries recordId redundantly for convenience.
 */
public class CdcEvent {

    public String eventId;                // UUID string
    public String entity;                 // "Employee"
    public String recordId;               // "EMP-000123"
    public String type;                   // "N" normal or "G" gap
    public String timestamp;              // ISO-8601
    public long replayId;                 // monotonically increasing-ish
    public Map<String, Object> changedFields; // only fields that changed
    public GapInfo gap;                   // present for type == "G"

    public CdcEvent() {}
}
