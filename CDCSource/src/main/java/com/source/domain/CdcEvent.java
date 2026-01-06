package com.source.domain;

import java.util.Map;

public class CdcEvent {
    public String eventId;        // UUID
    public String entity;         // "Employee"
    public String recordId;       // key
    public String type;           // "N" or "G"
    public String timestamp;      // ISO-8601 string
    public long replayId;         // monotonically increasing
    public Map<String, Object> changedFields;

    public GapInfo gap;           // present if type == "G"

    public CdcEvent() {}
}
