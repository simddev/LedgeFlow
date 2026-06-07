package dev.simd.ledgeflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

// @EnableKafka is explicit because KafkaConsumerConfig defines kafkaListenerContainerFactory,
// which disables the Spring Boot 4 auto-configuration class that would otherwise register it.
@SpringBootApplication
@EnableKafka
public class LedgeFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgeFlowApplication.class, args);
    }

}
