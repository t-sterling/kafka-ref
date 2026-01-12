package cdc.gap.handler.streams;

import cdc.gap.handler.config.GapHandlerMetrics;
import cdc.gap.handler.config.GapHandlerProps;
import cdc.gap.handler.domain.CdcEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


/**
 *
 * Test to write:
 *  cdc event is written to cdc-out
 *  cdc gap event causes fill-command
 *      - assert output
 *      - assert state store
 *
 *  cdc gap event times out
 *  cdc gap event buffer overflows
 *  fill-event flushes buffer, all events
 *  fill-event flushed buffer, old cdc skipped
 *  fill-event already timed out / buffer overflowed
 *  fill flushes events in order
 */

public class KafkaStreamsTest {

    private GapHandlerProps.Topics topics;
    private Topology topology;
    private Properties props;

    @BeforeEach
    void init(){
        this.topics = new GapHandlerProps.Topics(
                "cdc-in",
                "cdc-fill-event",
                "cdc-fill-command",
                "cdc-out"
        );
        var state = new GapHandlerProps.State("cdc-state", 1_000_00);
        var meterRegistry = Mockito.mock(GapHandlerMetrics.class);
        this.topology = new GapHandlerTopology().buildTopology(new ObjectMapper(), new GapHandlerProps(topics, state), meterRegistry);
        this.props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");

    }

    @Test
    void testSimpleTransformation() {


        var deserializer = new JsonDeserializer<>(CdcEvent.class);
        deserializer.addTrustedPackages("cdc.gap.handler.domain");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {

            var input = driver.createInputTopic(
                topics.cdcIn(),
                new StringSerializer(),
                new JsonSerializer<CdcEvent>()
            );

            var output = driver.createOutputTopic(
                topics.cdcOut(),
                new StringDeserializer(),
                deserializer
            );

            var event = new CdcEvent();
            event.recordId = "123";
            event.type = "N";

            input.pipeInput("123", event);

            assertThat(output.readValue().recordId).isEqualTo("123");
        }

    }
}