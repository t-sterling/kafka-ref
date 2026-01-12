package cdc.gap.handler.streams;

import cdc.gap.handler.config.GapHandlerMetrics;
import cdc.gap.handler.config.GapHandlerProps;
import cdc.gap.handler.domain.CdcEvent;
import cdc.gap.handler.domain.Either;
import cdc.gap.handler.domain.FillCommand;
import cdc.gap.handler.domain.FillEvent;
import cdc.gap.handler.serde.JsonSerdes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GapHandlerTopology {

    private static final Logger LOG = LoggerFactory.getLogger(GapHandlerTopology.class);

    static class ProcessorNames {

        public static final String GAP_HANDLER_PROCESSOR = "GapHandlerProcessor";
        public static final String CDC_EVENT_FORWARDER = "CdcEventForwarder";
        public static final String FILL_COMMAND_FORWARDER = "FillCommandForwarder";
        public static final String FILL_EVENT_PROCESSOR = "FillEventProcessor";

    }

    public Topology buildTopology(ObjectMapper mapper,
                                  GapHandlerProps gapHandlerProps,
                                  GapHandlerMetrics gapHandlerMetrics){

        var recordIdSerde = Serdes.String();
        var cdcEventSerde = JsonSerdes.jsonSerde(mapper, CdcEvent.class);
        var fillCommandSerde = JsonSerdes.jsonSerde(mapper, FillCommand.class);
        var fillEventSerde = JsonSerdes.jsonSerde(mapper, FillEvent.class);

        var stateSerde = JsonSerdes.jsonSerde(mapper, GapEventState.class);

        var topics = gapHandlerProps.topics();
        var topology = new Topology();

        // add cdc-in to the topology
        //
        topology.addSource(
                topics.cdcIn(),
                recordIdSerde.deserializer(),
                cdcEventSerde.deserializer(),
                topics.cdcIn()
        );

        // add the fill-event topic
        //
        topology.addSource(
                topics.cdcFillEvent(),
                recordIdSerde.deserializer(),
                fillEventSerde.deserializer(),
                topics.cdcFillEvent()
        );

        // link a processor which recognizes gap-events and forks the stream
        //
        topology.addProcessor(
                ProcessorNames.GAP_HANDLER_PROCESSOR,
                () -> new GapEventProcessor(
                    gapHandlerProps.state().storeName(),
                    ProcessorNames.CDC_EVENT_FORWARDER,
                    ProcessorNames.FILL_COMMAND_FORWARDER,
                    gapHandlerMetrics
                ),
                topics.cdcIn()
        );

        // add processors that just forward the correct type to the correct topic
        //
        topology.addProcessor(
                ProcessorNames.CDC_EVENT_FORWARDER,
                () -> new ForwarderProcessor<CdcEvent, CdcEvent, FillCommand>(
                    Either::left,
                    topics.cdcOut()
                ),
                ProcessorNames.GAP_HANDLER_PROCESSOR
        );
        topology.addProcessor(
                ProcessorNames.FILL_COMMAND_FORWARDER,
                () -> new ForwarderProcessor<FillCommand, CdcEvent, FillCommand>(
                    Either::right,
                    topics.cdcFillCommand()
                ),
                ProcessorNames.GAP_HANDLER_PROCESSOR
        );

        topology.addProcessor(
                ProcessorNames.FILL_EVENT_PROCESSOR,
                () -> new FillEventProcessor(
                    gapHandlerProps.state().storeName(),
                    topics.cdcOut(),
                    gapHandlerMetrics
                ),
                topics.cdcFillEvent()
        );

        topology.addSink(
                topics.cdcOut(),
                topics.cdcOut(),
                recordIdSerde.serializer(),
                cdcEventSerde.serializer(),
                ProcessorNames.CDC_EVENT_FORWARDER, ProcessorNames.FILL_EVENT_PROCESSOR
        );

        topology.addSink(
                topics.cdcFillCommand(),
                topics.cdcFillCommand(),
                recordIdSerde.serializer(),
                fillCommandSerde.serializer(),
                ProcessorNames.FILL_COMMAND_FORWARDER
        );

        // Add a store for buffering CDC events while the FillEvent is being created
        //
        var storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(gapHandlerProps.state().storeName()),
                recordIdSerde,
                stateSerde
        );

        topology.addStateStore(
            storeBuilder,
            ProcessorNames.GAP_HANDLER_PROCESSOR,
            ProcessorNames.FILL_EVENT_PROCESSOR
        );

        LOG.info("Topology built: {}", topology.describe());

        return topology;
    }

}
