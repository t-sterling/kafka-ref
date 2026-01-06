
## Spring docs
- https://docs.spring.io/spring-kafka/reference/
- https://docs.spring.io/spring-boot/reference/messaging/kafka.html

Reference:  
- [Simple consumer/producers](#Simple-consumers-and-producers)  
- [Kafka Streams DSL](#Kafka-Streams-DSL)  
- [Kafka Streams Processor API](#Kafka-Streams-Processor-API)

### Simple consumers and producers

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: demo-group
      auto-offset-reset: earliest
    producer:
      acks: all
```

```java
@Configuration
@EnableKafka
public class KafkaConfig {

  /**
   * Topic name used by both producer and consumer.
   * In real systems this usually lives in application.yml.
   */
  public static final String TOPIC = "demo-topic";

  /* ============================================================
   * PRODUCER SIDE
   * ============================================================
   */

  /**
   * ProducerFactory
   *
   * Purpose:
   *  - Owns the configuration required to create KafkaProducer instances.
   *  - KafkaTemplate does NOT create producers directly; it asks this factory.
   *
   * Why it is required:
   *  - KafkaProducer is stateful and expensive.
   *  - Spring uses this factory to manage producer lifecycle and reuse.
   *
   * Key points:
   *  - You define serializers here.
   *  - You define bootstrap servers and producer-level reliability settings.
   */
  @Bean
  public ProducerFactory<String, DemoEvent> producerFactory(ObjectMapper mapper) {

    Map<String, Object> props = new HashMap<>();

    // Where the Kafka cluster lives
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // How keys are serialized
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    // How values are serialized (JSON in this example)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    // Reliability: wait for all in-sync replicas
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    return new DefaultKafkaProducerFactory<>(
            props,
            new StringSerializer(),
            new JsonSerializer<>(mapper)
    );
  }

  /**
   * KafkaTemplate
   *
   * Purpose:
   *  - High-level abstraction used by application code to publish messages.
   *
   * Why it is required:
   *  - Encapsulates KafkaProducer usage.
   *  - Handles serialization, retries, async sending, and metrics.
   *
   * Mental model:
   *  - "KafkaTemplate is to KafkaProducer what JdbcTemplate is to JDBC."
   */
  @Bean
  public KafkaTemplate<String, DemoEvent> kafkaTemplate(
          ProducerFactory<String, DemoEvent> producerFactory) {

    return new KafkaTemplate<>(producerFactory);
  }

  /* ============================================================
   * CONSUMER SIDE
   * ============================================================
   */

  /**
   * ConsumerFactory
   *
   * Purpose:
   *  - Owns the configuration required to create KafkaConsumer instances.
   *
   * Why it is required:
   *  - @KafkaListener containers do NOT create consumers directly.
   *  - Spring uses this factory to manage consumer lifecycle and threading.
   *
   * Key points:
   *  - You define deserializers here.
   *  - You define group.id and offset behavior here.
   */
  @Bean
  public ConsumerFactory<String, DemoEvent> consumerFactory(ObjectMapper mapper) {

    Map<String, Object> props = new HashMap<>();

    // Where the Kafka cluster lives
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // Consumer group id (defines parallelism + offset ownership)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-group");

    // Where to start if no committed offset exists
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new JsonDeserializer<>(DemoEvent.class, mapper, false)
    );
  }

  /**
   * KafkaListenerContainerFactory
   *
   * Purpose:
   *  - Creates MessageListenerContainer instances for @KafkaListener methods.
   *
   * Why it is required:
   *  - @KafkaListener is declarative, but something must:
   *      - start the poll loop
   *      - manage threads
   *      - handle rebalances
   *      - commit offsets
   *
   * This factory defines HOW listeners run.
   *
   * Mental model:
   *  - One @KafkaListener method → one MessageListenerContainer
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, DemoEvent>
  kafkaListenerContainerFactory(ConsumerFactory<String, DemoEvent> consumerFactory) {

    ConcurrentKafkaListenerContainerFactory<String, DemoEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

    // Tell the container how to create KafkaConsumer instances
    factory.setConsumerFactory(consumerFactory);

    // Optional but common tuning points:
    // factory.setConcurrency(3);        // parallel consumers
    // factory.getContainerProperties().setAckMode(AckMode.BATCH);

    return factory;
  }

  /* ============================================================
   * LISTENER
   * ============================================================
   */

  /**
   * @KafkaListener method
   *
   * Purpose:
   *  - Application-level message handler.
   *
   * How it works:
   *  - Spring detects this method at startup.
   *  - It creates a MessageListenerContainer using the factory above.
   *  - That container:
   *      - polls Kafka
   *      - deserializes records
   *      - invokes this method
   *
   * Important:
   *  - This method runs on a Kafka consumer thread.
   *  - Blocking here blocks partition progress.
   */
  @KafkaListener(
          topics = TOPIC,
          containerFactory = "kafkaListenerContainerFactory"
  )
  public void onMessage(DemoEvent event) {
    System.out.println("Received event: " + event);
  }
}
```

##### Kafka Streams DSL

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    streams:
      application-id: demo-streams-app
      properties:
        # optional but common
        processing.guarantee: at_least_once
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde

topics:
  input: demo-input
  output: demo-output
```

```java
@Configuration
@EnableKafkaStreams
public class KafkaStreamsDslConfig {

    @Value("${topics.input}")
    private String inputTopic;

    @Value("${topics.output}")
    private String outputTopic;

    /**
     * This bean is OPTIONAL in Boot if you only use application.yml,
     * but it's canonical when you want to explicitly control StreamsConfig.
     *
     * If you omit it, Spring Boot builds a KafkaStreamsConfiguration from
     * spring.kafka.streams.* properties.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Topology definition using the Kafka Streams DSL.
     *
     * Spring will:
     *  - create a StreamsBuilder (via StreamsBuilderFactoryBean)
     *  - call this @Bean method, injecting that StreamsBuilder
     *  - start KafkaStreams with the built topology
     */
    @Bean
    public KStream<String, DemoEvent> demoTopology(StreamsBuilder builder, ObjectMapper mapper) {

        var keySerde = Serdes.String();
        var eventSerde = JsonSerdes.jsonSerde(mapper, DemoEvent.class);

        KStream<String, DemoEvent> stream =
                builder.stream(inputTopic, Consumed.with(keySerde, eventSerde));

        // minimal example: uppercase payload
        stream
            .mapValues(evt -> new DemoEvent(evt.id(), evt.payload().toUpperCase()))
            .to(outputTopic, Produced.with(keySerde, eventSerde));

        // Returning the stream is optional; many teams return void.
        // Keeping it returned is useful for testing/visibility.
        return stream;
    }
}
```

##### Kafka Streams Processor API

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    streams:
      application-id: demo-processor-app
      properties:
        processing.guarantee: at_least_once

topics:
  input: demo-input
  output: demo-output
```

```java
@Configuration
public class KafkaStreamsProcessorApiConfig {

    @Value("${spring.kafka.streams.application-id}")
    private String appId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${topics.input}")
    private String inputTopic;

    @Value("${topics.output}")
    private String outputTopic;

    /**
     * Streams config (same logical role as in the DSL example).
     * This is passed directly to the KafkaStreams runtime.
     */
    @Bean
    public Properties streamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        return props;
    }

    /**
     * Processor API: we build an explicit Topology graph:
     *
     * source -> processor -> sink
     */
    @Bean
    public Topology topology(ObjectMapper mapper) {

        var keySerde = Serdes.String();
        var valueSerde = JsonSerdes.jsonSerde(mapper, DemoEvent.class);

        Topology t = new Topology();

        t.addSource(
                "source",
                keySerde.deserializer(),
                valueSerde.deserializer(),
                inputTopic
        );

        t.addProcessor(
                "uppercaser",
                UppercaseProcessor::new,
                "source"
        );

        t.addSink(
                "sink",
                outputTopic,
                keySerde.serializer(),
                valueSerde.serializer(),
                "uppercaser"
        );

        return t;
    }

    /**
     * This is the crucial difference vs the DSL/Spring auto setup:
     *
     * - With @EnableKafkaStreams, Spring creates KafkaStreams for you.
     * - With a Topology + Processor API, YOU create KafkaStreams and
     *   let Spring manage its lifecycle (start/close).
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    public KafkaStreams kafkaStreams(Topology topology, Properties streamsProperties) {
        return new KafkaStreams(topology, streamsProperties);
    }

    /**
     * Minimal processor: transforms the record value and forwards it.
     * Note: forwarding only moves the record to the downstream node ("sink" here).
     */
    static class UppercaseProcessor
            extends ContextualProcessor<String, DemoEvent, String, DemoEvent> {

        @Override
        public void process(Record<String, DemoEvent> record) {
            DemoEvent v = record.value();
            DemoEvent updated = new DemoEvent(v.id(), v.payload().toUpperCase());

            // Forward to child node(s) (the sink node writes to the output topic)
            context().forward(record.withValue(updated));
        }
    }
}
```