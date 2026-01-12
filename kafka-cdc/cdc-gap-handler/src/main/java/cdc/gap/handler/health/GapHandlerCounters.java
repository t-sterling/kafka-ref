package cdc.gap.handler.health;

import cdc.gap.handler.config.GapHandlerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;

//@Endpoint(id = "counters")
//@Component
public class GapHandlerCounters {

    private final MeterRegistry meterRegistry;

    public GapHandlerCounters(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @ReadOperation
    public Map<String, Object> counters() {

        var map = new TreeMap<String, Object>();

        addEntry(map, GapHandlerMetrics.CDC_EVENT_RECEIVED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_EVENT_FORWARDED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_EVENT_DROPPED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_EVENT_FLUSHED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_EVENT_BUFFERED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_EVENT_FILLED_COUNT);

        addEntry(map, GapHandlerMetrics.CDC_GAP_EVENT_DETECTED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_GAP_EVENT_DROPPED);

        addEntry(map, GapHandlerMetrics.CDC_FILL_COMMAND_PUBLISHED_COUNT);
        addEntry(map, GapHandlerMetrics.CDC_FILL_COMMAND_TIMEOUT);
        addEntry(map, GapHandlerMetrics.CDC_FILL_EVENT_RECEIVED_COUNT);

        map.put("NOT_NORMAL", notNormalEvents());
        map.put("GAP_DETECTED_OR_BUFFERED", detectedOrBuffered());
        map.put("NOT_NORMAL=GAP_DETECTED_OR_BUFFERED", notNormalEvents() == detectedOrBuffered());

        return map;

    }

    private void addEntry(Map<String, Object> map, String name) {
        map.put(name, count(name));
    }

    private double count( String name){
        var c = this.meterRegistry.find(name).counter();
        return (c == null) ? 0.0 : c.count();
    }

    private double notNormalEvents(){
        return count(GapHandlerMetrics.CDC_EVENT_RECEIVED_COUNT) - count(GapHandlerMetrics.CDC_EVENT_FORWARDED_COUNT);
    }

    private double detectedOrBuffered(){
        return count(GapHandlerMetrics.CDC_GAP_EVENT_DETECTED_COUNT) + count(GapHandlerMetrics.CDC_EVENT_BUFFERED_COUNT);
    }

    /*
    1: assert that all non forward cdc events ARE either gap events OR buffered while a Fill event is requested
     cdc.events.received - cdc.events.forwarded == cdc.gap-event.detected + cdc.events.buffered

    2: assert that

    {
      "cdc.events.buffered": 3,
      "cdc.events.filled": 15,
      "cdc.events.flushed": 0,
      "cdc.events.forwarded": 91,
      "cdc.events.received": 114,
      "cdc.events.skipped": 15,
      "cdc.fill-commands.published": 20,
      "cdc.fill-event.received": 15,
      "cdc.gap-event.detected": 20,
      "gapAffected": 23
    }

    {
      "cdc.events.buffered": 10,
      "cdc.events.filled": 20,
      "cdc.events.flushed": 0,
      "cdc.events.forwarded": 161,
      "cdc.events.received": 191,
      "cdc.events.skipped": 0,
      "cdc.fill-commands.published": 20,
      "cdc.fill-event.received": 20,
      "cdc.gap-event.detected": 20,
      "gapAffected": 30
    }
     */

}
