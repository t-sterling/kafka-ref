package com.source.domain;

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
