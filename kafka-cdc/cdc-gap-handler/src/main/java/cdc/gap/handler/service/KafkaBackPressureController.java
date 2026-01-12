package cdc.gap.handler.service;

import cdc.gap.handler.config.GapHandlerProps;
import cdc.gap.handler.domain.FillCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This controls back pressure from Kafka if/when the FillService is failing
 * When too many commands fail and the circuit opens the Kafka consumer will be paused. (this doesn't cause a rebalance since heartbeats are maintained)
 *
 * The FillService should
 */
@Component
public class KafkaBackPressureController {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBackPressureController.class);

    private static final double JITTER = 0.20;

    private final KafkaListenerEndpointRegistry registry;
    private final FillerService fillerService;
    private final ScheduledExecutorService scheduler;
    private final GapHandlerProps.Retries retries;

    // state
    private volatile Instant outageStart;
    private volatile Duration backoff;
    private final AtomicBoolean retryLoopRunning;
    private final Duration maxOutage;

    public KafkaBackPressureController(FillerService fillerService,
                                       GapHandlerProps.Retries retries,
                                       KafkaListenerEndpointRegistry registry) {
        this.fillerService = fillerService;
        this.retries = retries;
        this.registry = registry;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "fill-retry"));
        this.retryLoopRunning = new AtomicBoolean(false);
        this.maxOutage = Duration.ofMinutes(retries.maxOutageMinutes());
    }

    @KafkaListener(
            id = "cdc-gap-handler-worker",
            topics = "#{'${gap-handler.topics.cdc-fill-command}'}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(@Payload FillCommand command, Acknowledgment ack) {
        try {
            this.fillerService.processCommand(command);
            ack.acknowledge();
            processingSucceeded();
        } catch (TransientFillException e) {
            LOG.info(e.getMessage(), e);
            processingFailed(e, command);
        }
    }

    private void processingSucceeded() {
        this.outageStart = null;
        this.backoff = Duration.ofSeconds(this.retries.initialBackoffSeconds());
        this.retryLoopRunning.set(false);
        resume();
    }

    /**
     * resume the kafka subscription
     */
    private void resume() {
        var container = messageListenerContainer();
        if (container != null) {
            container.resume();
        }
    }

    private void processingFailed(Exception e, FillCommand command) {
        if (this.outageStart == null) {
            this.outageStart = Instant.now();
        }
        pause();
        startRetryLoop(command);
    }

    /**
     * Pause the Kafka subscription without rebalancing
     */
    private void pause() {
        LOG.warn("pausing filer-service subscription due to transient failure.");
        var container = messageListenerContainer();
        if (container != null) {
            container.pause();
        }
    }

    private MessageListenerContainer messageListenerContainer() {
        return registry.getListenerContainer("cdc-gap-handler-worker");
    }

    private void startRetryLoop(FillCommand command) {
        if (retryLoopRunning.compareAndSet(false, true)) {
            scheduleRetry(command);
        }
    }

    /**
     * note: these retries will NOT acknowledge the message from Kafka, i.e. it WILL be processed again
     * once the subscription is resumed.
     * @param command
     */
    private void scheduleRetry(FillCommand command){
        var delay = withJitter(Duration.ofSeconds(this.retries.initialBackoffSeconds()));
        scheduler.schedule(() -> {

            try {

                if (outageStart != null && Duration.between(outageStart, Instant.now()).compareTo(maxOutage) > 0) {
                    shutdownHard("Fill dependency down > " + maxOutage);
                    return;
                }

                // retry the command
                //
                fillerService.processCommand(command);
                processingSucceeded();

            } catch (Exception ignore) {

                // Still down => pause stays in effect; increase backoff and try again
                this.backoff = nextBackoff(backoff);
                scheduleRetry(command);

            }

        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }


    private Duration withJitter(Duration base) {
        long ms = base.toMillis();
        long jitter = (long) (ms * JITTER);
        long delta = ThreadLocalRandom.current().nextLong(-jitter, jitter + 1);
        return Duration.ofMillis(Math.max(0, ms + delta));
    }

    private Duration nextBackoff(Duration current) {
        var maxBackOffMillis = Duration.ofSeconds(this.retries.maxBackOffSeconds()).toMillis();
        long next = Math.min(maxBackOffMillis, current.toMillis() * 2);
        return Duration.ofMillis(next);
    }

    private void shutdownHard(String reason) {
        LOG.error("shutting down hard: {}", reason);
        Runtime.getRuntime().halt(1);
    }

}