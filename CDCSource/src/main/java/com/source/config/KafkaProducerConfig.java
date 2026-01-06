package com.source.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@Configuration
@EnableKafka
public class KafkaProducerConfig {
    // Spring Boot autoconfig + application.yml is enough for StringSerializer.
}
