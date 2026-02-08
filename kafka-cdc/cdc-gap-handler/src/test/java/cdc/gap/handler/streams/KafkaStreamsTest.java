package cdc.gap.handler.streams;

import cdc.gap.handler.config.GapHandlerMetrics;
import cdc.gap.handler.config.GapHandlerProps;
import cdc.gap.handler.domain.CdcEvent;
import cdc.gap.handler.domain.FillCommand;
import cdc.gap.handler.domain.FillEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;


import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


public class KafkaStreamsTest {

    private GapHandlerProps.Topics topics;
    private GapHandlerProps gapHandlerProps;
    private Topology topology;
    private Properties props;
    private TopologyTestDriver driver;

    private TestInputTopic<String, CdcEvent> cdcInput;
    private TestInputTopic<String, FillEvent> fillEventInput;
    private TestOutputTopic<String, CdcEvent> cdcOutput;
    private TestOutputTopic<String, FillCommand> fillCommandOutput;

    @BeforeEach
    void init() {
        this.topics = new GapHandlerProps.Topics(
                "cdc-in",
                "cdc-fill-event",
                "cdc-fill-command",
                "cdc-out"
        );
        var state = new GapHandlerProps.State("cdc-state", 1_000_000, 60, 120);
        var meterRegistry = Mockito.mock(GapHandlerMetrics.class);
        var retries = new GapHandlerProps.Retries(0, 0, 0);
        this.gapHandlerProps = new GapHandlerProps(topics, state, retries, "");
        this.topology = new GapHandlerTopology().buildTopology(new ObjectMapper(), gapHandlerProps, meterRegistry);

        this.props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");

        this.driver = new TopologyTestDriver(topology, props);

        // Input topics
        this.cdcInput = driver.createInputTopic(
                topics.cdcIn(),
                new StringSerializer(),
                new JsonSerializer<>()
        );

        var fillEventDeserializer = new JsonDeserializer<>(FillEvent.class);
        fillEventDeserializer.addTrustedPackages("cdc.gap.handler.domain");
        this.fillEventInput = driver.createInputTopic(
                topics.cdcFillEvent(),
                new StringSerializer(),
                new JsonSerializer<>()
        );

        // Output topics
        var cdcEventDeserializer = new JsonDeserializer<>(CdcEvent.class);
        cdcEventDeserializer.addTrustedPackages("cdc.gap.handler.domain");
        this.cdcOutput = driver.createOutputTopic(
                topics.cdcOut(),
                new StringDeserializer(),
                cdcEventDeserializer
        );

        var fillCommandDeserializer = new JsonDeserializer<>(FillCommand.class);
        fillCommandDeserializer.addTrustedPackages("cdc.gap.handler.domain");
        this.fillCommandOutput = driver.createOutputTopic(
                topics.cdcFillCommand(),
                new StringDeserializer(),
                fillCommandDeserializer
        );
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private KeyValueStore<String, GapEventState> getStateStore() {
        return driver.getKeyValueStore(gapHandlerProps.state().storeName());
    }

    private CdcEvent createCdcEvent(String recordId, String type, long timestamp) {
        var event = new CdcEvent();
        event.recordId = recordId;
        event.eventId = "evt-" + System.nanoTime();
        event.entity = "Employee";
        event.type = type;
        event.timestamp = timestamp;
        event.changedFields = Map.of("field1", "value1");
        return event;
    }

    private FillEvent createFillEvent(String recordId, long cutoffTimestamp) {
        return new FillEvent(
                recordId,
                "Employee",
                "fill-evt-" + System.nanoTime(),
                cutoffTimestamp,
                Map.of("allField1", "allValue1"),
                true,
                null
        );
    }

    // ========================================================================
    // Normal Flow Tests
    // ========================================================================

    @Nested
    @DisplayName("Normal CDC Flow")
    class NormalFlowTests {

        @Test
        @DisplayName("Normal CDC event flows directly to output")
        void normalCdcEventFlowsToOutput() {
            var event = createCdcEvent("record-1", "N", 1000L);

            cdcInput.pipeInput("record-1", event);

            assertThat(cdcOutput.isEmpty()).isFalse();
            var output = cdcOutput.readValue();
            assertThat(output.recordId).isEqualTo("record-1");
            assertThat(output.type).isEqualTo("N");

            // No fill command should be sent
            assertThat(fillCommandOutput.isEmpty()).isTrue();

            // No state should be created
            assertThat(getStateStore().get("record-1")).isNull();
        }

        @Test
        @DisplayName("Multiple normal CDC events for different records all flow through")
        void multipleNormalEventsFlowThrough() {
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 1000L));
            cdcInput.pipeInput("record-2", createCdcEvent("record-2", "N", 1001L));
            cdcInput.pipeInput("record-3", createCdcEvent("record-3", "N", 1002L));

            var outputs = cdcOutput.readValuesToList();
            assertThat(outputs).hasSize(3);
            assertThat(outputs).extracting(e -> e.recordId)
                    .containsExactly("record-1", "record-2", "record-3");
        }
    }

    // ========================================================================
    // Invariant 3: Ordering Within a Record
    // ========================================================================

    @Nested
    @DisplayName("Invariant 3: Ordering Within a Record")
    class OrderingTests {

        @Test
        @DisplayName("Buffered events are flushed in arrival order")
        void bufferedEventsAreFlushedInOrder() {
            // Send gap event
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));

            // Buffer multiple events
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 3000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 4000L));

            // Clear fill command output
            fillCommandOutput.readValuesToList();
            assertThat(cdcOutput.isEmpty()).isTrue();

            // Send fill event with cutoff before all buffered events
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1500L));

            // Should get fill event + 3 buffered events in order
            var outputs = cdcOutput.readValuesToList();
            assertThat(outputs).hasSize(4);

            // First is the fill event (type "F")
            assertThat(outputs.get(0).type).isEqualTo("F");

            // Remaining are buffered events in FIFO order (pollFirst)
            assertThat(outputs.get(1).timestamp).isEqualTo(2000L);
            assertThat(outputs.get(2).timestamp).isEqualTo(3000L);
            assertThat(outputs.get(3).timestamp).isEqualTo(4000L);
        }
    }

    // ========================================================================
    // Invariant 4: Idempotent Fill Processing
    // ========================================================================

    @Nested
    @DisplayName("Invariant 4: Idempotent Fill Processing")
    class IdempotentFillTests {

        @Test
        @DisplayName("Fill event with no corresponding buffer is safely ignored")
        void fillEventWithNoBufferIsIgnored() {
            // Send fill event for a record that was never in gap
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1000L));

            // No output should be produced
            assertThat(cdcOutput.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Duplicate fill event after buffer already flushed is safely ignored")
        void duplicateFillEventIsIgnored() {
            // Create gap and buffer
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));

            // First fill - should flush
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1500L));
            var firstFlush = cdcOutput.readValuesToList();
            assertThat(firstFlush).hasSize(2); // fill + 1 buffered

            // Duplicate fill - should be ignored
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1500L));
            assertThat(cdcOutput.isEmpty()).isTrue();

            // State should be cleared
            assertThat(getStateStore().get("record-1")).isNull();
        }
    }

    // ========================================================================
    // Invariant 5: Timestamp-based Deduplication on Flush
    // ========================================================================

    @Nested
    @DisplayName("Invariant 5: Timestamp-based Deduplication on Flush")
    class TimestampDeduplicationTests {

        @Test
        @DisplayName("Buffered events older than cutoff are dropped")
        void oldBufferedEventsAreDropped() {
            // Send gap event
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));

            // Buffer events with various timestamps
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 1500L)); // older than cutoff
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L)); // equal to cutoff
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2500L)); // newer than cutoff
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 3000L)); // newer than cutoff

            // Clear outputs
            fillCommandOutput.readValuesToList();

            // Send fill event with cutoff at 2000
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 2000L));

            var outputs = cdcOutput.readValuesToList();

            // Should get: fill event + only events with timestamp > 2000 (2500, 3000)
            assertThat(outputs).hasSize(3);
            assertThat(outputs.get(0).type).isEqualTo("F"); // fill event

            // Only timestamps > 2000 should be present
            var bufferedTimestamps = outputs.stream()
                    .skip(1) // skip fill event
                    .map(e -> e.timestamp)
                    .toList();
            assertThat(bufferedTimestamps).allMatch(ts -> ts > 2000L);
        }

        @Test
        @DisplayName("All buffered events are dropped if all older than cutoff")
        void allOldEventsDropped() {
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 1500L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 1800L));

            fillCommandOutput.readValuesToList();

            // Fill with cutoff after all buffered events
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 5000L));

            var outputs = cdcOutput.readValuesToList();
            // Only the fill event should be output
            assertThat(outputs).hasSize(1);
            assertThat(outputs.get(0).type).isEqualTo("F");
        }
    }

    // ========================================================================
    // Invariant 8: Gap Event Deduplication
    // ========================================================================

    @Nested
    @DisplayName("Invariant 8: Gap Event Deduplication")
    class GapDeduplicationTests {

        @Test
        @DisplayName("Gap event creates state and sends FillCommand")
        void gapEventCreatesStateAndSendsFillCommand() {
            var gapEvent = createCdcEvent("record-1", "G", 1000L);
            cdcInput.pipeInput("record-1", gapEvent);

            // Should create state
            var state = getStateStore().get("record-1");
            assertThat(state).isNotNull();
            assertThat(state.inGap).isTrue();

            // Should send fill command
            assertThat(fillCommandOutput.isEmpty()).isFalse();
            var fillCommand = fillCommandOutput.readValue();
            assertThat(fillCommand.recordId()).isEqualTo("record-1");

            // Should NOT forward to cdc output
            assertThat(cdcOutput.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Duplicate gap events are dropped - only one FillCommand sent")
        void duplicateGapEventsAreDropped() {
            // Send multiple gap events for same record
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1001L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1002L));

            // Only ONE fill command should be sent
            var fillCommands = fillCommandOutput.readValuesToList();
            assertThat(fillCommands).hasSize(1);

            // No CDC output
            assertThat(cdcOutput.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Normal events after gap are buffered, not forwarded")
        void normalEventsAfterGapAreBuffered() {
            // Gap event
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            fillCommandOutput.readValuesToList(); // clear

            // Normal events while in gap
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 3000L));

            // Should NOT appear in output yet
            assertThat(cdcOutput.isEmpty()).isTrue();

            // Should be in buffer
            var state = getStateStore().get("record-1");
            assertThat(state.buffer).hasSize(2);
        }
    }

    // ========================================================================
    // Cross-Record Isolation
    // ========================================================================

    @Nested
    @DisplayName("Cross-Record Isolation")
    class CrossRecordIsolationTests {

        @Test
        @DisplayName("Gap in one record does not affect other records")
        void gapDoesNotAffectOtherRecords() {
            // Record 1 enters gap
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));

            // Record 2 normal events should flow through
            cdcInput.pipeInput("record-2", createCdcEvent("record-2", "N", 1001L));
            cdcInput.pipeInput("record-2", createCdcEvent("record-2", "N", 1002L));

            // Record 1 buffered event
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 1003L));

            // Only record-2 events should be in output
            var outputs = cdcOutput.readValuesToList();
            assertThat(outputs).hasSize(2);
            assertThat(outputs).allMatch(e -> e.recordId.equals("record-2"));

            // Record 1 should have buffered event
            var state1 = getStateStore().get("record-1");
            assertThat(state1.buffer).hasSize(1);

            // Record 2 should have no state
            assertThat(getStateStore().get("record-2")).isNull();
        }

        @Test
        @DisplayName("Fill for one record does not affect other records in gap")
        void fillDoesNotAffectOtherRecords() {
            // Both records enter gap
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            cdcInput.pipeInput("record-2", createCdcEvent("record-2", "G", 1001L));

            // Buffer events for both
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));
            cdcInput.pipeInput("record-2", createCdcEvent("record-2", "N", 2001L));

            fillCommandOutput.readValuesToList(); // clear

            // Fill only record-1
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1500L));

            // Record-1 should be flushed
            var outputs = cdcOutput.readValuesToList();
            assertThat(outputs).hasSize(2); // fill + buffered
            assertThat(outputs).allMatch(e -> e.recordId.equals("record-1"));

            // Record-1 state should be cleared
            assertThat(getStateStore().get("record-1")).isNull();

            // Record-2 should still be in gap with buffer
            var state2 = getStateStore().get("record-2");
            assertThat(state2).isNotNull();
            assertThat(state2.inGap).isTrue();
            assertThat(state2.buffer).hasSize(1);
        }
    }

    // ========================================================================
    // Invariant 1 & 2: Co-partitioning and Single-threaded Access
    // ========================================================================

    @Nested
    @DisplayName("Invariant 1 & 2: Co-partitioning Assumptions")
    class CoPartitioningTests {

        /**
         * This test documents the CRITICAL co-partitioning requirement.
         *
         * The topology has two source topics (CDC-INPUT, CDC-FILL-EVENT) that
         * share a state store. For correctness:
         *
         * 1. Both topics MUST have the same number of partitions
         * 2. Both topics MUST use the same partitioning key (recordId)
         * 3. The partitioner MUST assign the same key to the same partition number
         *
         * If violated: FillEvent arrives at a different task than the one holding
         * the buffer, causing silent data loss (fill is ignored, buffer grows forever).
         *
         * This cannot be tested with TopologyTestDriver (single partition).
         * Must be validated at deployment time.
         */
        @Test
        @DisplayName("Fill event uses same key as gap event - required for co-partitioning")
        void fillEventKeyMatchesGapEventKey() {
            var recordId = "record-1";

            // Gap event keyed by recordId
            cdcInput.pipeInput(recordId, createCdcEvent(recordId, "G", 1000L));

            // Fill event MUST use same key
            fillEventInput.pipeInput(recordId, createFillEvent(recordId, 1500L));

            // If keys match (co-partitioned), fill should find and clear state
            assertThat(getStateStore().get(recordId)).isNull();
        }

        @Test
        @DisplayName("Fill event with wrong key finds no state - simulates partition mismatch")
        void fillEventWrongKeyFindsNoState() {
            // Gap event for record-1
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));

            // Fill event arrives with DIFFERENT key (simulates partition mismatch)
            // In production this would happen if topics aren't co-partitioned
            fillEventInput.pipeInput("record-WRONG", createFillEvent("record-1", 1500L));

            // State is NOT cleared - buffer orphaned forever
            var state = getStateStore().get("record-1");
            assertThat(state).isNotNull();
            assertThat(state.inGap).isTrue();
            assertThat(state.buffer).hasSize(1);

            // No output produced
            assertThat(cdcOutput.isEmpty()).isTrue();
        }
    }

    // ========================================================================
    // End-to-End Scenarios
    // ========================================================================

    @Nested
    @DisplayName("End-to-End Scenarios")
    class EndToEndTests {

        @Test
        @DisplayName("Full gap lifecycle: gap -> buffer -> fill -> flush")
        void fullGapLifecycle() {
            // 1. Normal event flows through
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 500L));
            assertThat(cdcOutput.readValuesToList()).hasSize(1);

            // 2. Gap event triggers fill command
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            var fillCmd = fillCommandOutput.readValue();
            assertThat(fillCmd.recordId()).isEqualTo("record-1");
            assertThat(cdcOutput.isEmpty()).isTrue();

            // 3. Events are buffered during gap
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 3000L));
            assertThat(cdcOutput.isEmpty()).isTrue();
            assertThat(getStateStore().get("record-1").buffer).hasSize(2);

            // 4. Fill event flushes everything
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1500L));
            var flushed = cdcOutput.readValuesToList();
            assertThat(flushed).hasSize(3); // fill + 2 buffered

            // 5. State is cleared
            assertThat(getStateStore().get("record-1")).isNull();

            // 6. New events flow normally again
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 4000L));
            assertThat(cdcOutput.readValuesToList()).hasSize(1);
        }

        @Test
        @DisplayName("Multiple gap cycles for same record")
        void multipleGapCycles() {
            // First gap cycle
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 1000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 2000L));
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 1500L));

            fillCommandOutput.readValuesToList();
            cdcOutput.readValuesToList();

            // Second gap cycle
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "G", 3000L));
            cdcInput.pipeInput("record-1", createCdcEvent("record-1", "N", 4000L));

            // Should have new fill command
            assertThat(fillCommandOutput.readValuesToList()).hasSize(1);

            // Should have new state
            var state = getStateStore().get("record-1");
            assertThat(state).isNotNull();
            assertThat(state.buffer).hasSize(1);

            // Fill second gap
            fillEventInput.pipeInput("record-1", createFillEvent("record-1", 3500L));
            var outputs = cdcOutput.readValuesToList();
            assertThat(outputs).hasSize(2);

            assertThat(getStateStore().get("record-1")).isNull();
        }
    }
}
