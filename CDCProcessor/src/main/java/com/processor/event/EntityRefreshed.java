package com.processor.event;


/**
 * Produced by a "refresher" service when a full entity reload is complete.
 *
 * cutoffLastModifiedEpochMs means:
 *   "Any CDC event older than this is considered stale and should be dropped,
 *    because the full refresh represents a more authoritative/current state."
 */
public record EntityRefreshed(String recordId, long cutoffLastModifiedEpochMs){
}