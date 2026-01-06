package cdc.gap.handler.domain;

import java.util.Map;

public class CdcEvent {

    public String eventId;                    // UUID string
    public String entity;                     // "Employee"
    public String recordId;                   // "EMP-000123"
    public String type;                       // "N" normal or "G" gap
    public Long timestamp;                    // ISO-8601
    public long replayId;                     // monotonically increasing-ish
    public Map<String, Object> changedFields; // only fields that changed
    public GapInfo gap;                       // present for type == "G"

    public CdcEvent() {}

    @Override
    public String toString() {
        return "CdcEvent{" +
                "eventId='" + eventId + '\'' +
                ", entity='" + entity + '\'' +
                ", recordId='" + recordId + '\'' +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", replayId=" + replayId +
                ", changedFields=" + changedFields +
                ", gap=" + gap +
                '}';
    }
}
