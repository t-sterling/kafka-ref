package cdc.gap.handler.streams;

import cdc.gap.handler.domain.Either;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.Function;

/**
 * this basically just exists to keep the generics happy. see the topology config
 *
 * @param <T> the thing you want to emit, should be either a U or a V
 * @param <U> a thing you might get
 * @param <V> another thing you might get
 */
public class ForwarderProcessor<T, U, V> extends ContextualProcessor<String, Either<U, V>, String, T> {

    /**
     * a lambda that knows how to get the T
     */
    private final Function<Either<U,V>, T> extractor;
    private final String outputTopic;

    private ProcessorContext<String, T> context;

    public ForwarderProcessor(Function<Either<U, V>, T> extractor, String outputTopic) {
        this.extractor = extractor;
        this.outputTopic = outputTopic;
    }

    @Override
    public void init(ProcessorContext<String, T> context) {
        super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, Either<U, V>> record) {
        var value = this.extractor.apply(record.value());
        this.context.forward(record.withValue(value), this.outputTopic);
    }

}
