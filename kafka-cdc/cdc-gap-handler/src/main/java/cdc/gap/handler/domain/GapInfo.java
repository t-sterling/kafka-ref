package cdc.gap.handler.domain;


/**
 * Only present on GAP events.
 * Used to simulate Salesforce-like "replayId gap detected".
 */
public class GapInfo {
    public long fromReplayId;
    public long toReplayId;
    public String reason;

    public GapInfo() {}

    public GapInfo(long fromReplayId, long toReplayId, String reason) {
        this.fromReplayId = fromReplayId;
        this.toReplayId = toReplayId;
        this.reason = reason;
    }
}
