package dev.simd.ledgeflow.kafka;

import dev.simd.ledgeflow.config.KafkaTopicConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String accountId, String eventJson) {
        kafkaTemplate.send(KafkaTopicConfig.ACCOUNT_EVENTS, accountId, eventJson);
    }
}
