package cdc.gap.handler.config;

import io.micrometer.core.instrument.MeterRegistry;

public class GapHandlerMetrics {

    /** how many cdc events have we received */
    public static final String CDC_EVENT_RECEIVED_COUNT = "cdc.events.received";

    /** how many normal cdc events just passed through */
    public static final String CDC_EVENT_FORWARDED_COUNT = "cdc.events.forwarded";

    /** how many fills have we published to the output topic */
    public static final String CDC_EVENT_FILLED_COUNT = "cdc.gap-events.filled";

    /** how many gap events were detected */
    public static final String CDC_GAP_EVENT_DETECTED_COUNT = "cdc.gap-event.detected";

    /** how many gap events did we drop because we were already processing a gap event */
    public static final String CDC_GAP_EVENT_DROPPED = "cdc.gap-event.dropped";

    /** how many cdc events got buffered because a fill-event was pending */
    public static final String CDC_EVENT_BUFFERED_COUNT = "cdc.events.buffered";

    /** how many buffered cdc events did we drop because the fill event was more recent */
    public static final String CDC_EVENT_DROPPED_COUNT = "cdc.buffered-events.dropped";

    /** how many buffered cdc events did we publish after the fill event */
    public static final String CDC_EVENT_FLUSHED_COUNT = "cdc.buffered-events.flushed";

    /** how many fill-command did we publish */
    public static final String CDC_FILL_COMMAND_PUBLISHED_COUNT = "cdc.fill-commands.published";

    /** how many fill-command timed-out */
    public static final String CDC_FILL_COMMAND_TIMEOUT = "cdc.fill-commands.timeout";

    /** how many fill-commands were re-sent due to stale gap recovery */
    public static final String CDC_FILL_COMMAND_RESENT_COUNT = "cdc.fill-commands.resent";

    /** how many fill-events did we receive */
    public static final String CDC_FILL_EVENT_RECEIVED_COUNT = "cdc.fill-event.received";

    private final MeterRegistry meterRegistry;

    public GapHandlerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void count(String metricName){
        this.meterRegistry.counter(metricName).increment();
    }

}
