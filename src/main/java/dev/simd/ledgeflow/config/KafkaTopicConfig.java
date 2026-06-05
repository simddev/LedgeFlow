package dev.simd.ledgeflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ACCOUNT_EVENTS = "account.events";
    public static final String ACCOUNT_ALERTS = "account.alerts";

    @Bean
    public NewTopic accountEventsTopic() {
        return TopicBuilder.name(ACCOUNT_EVENTS).build();
    }

    @Bean
    public NewTopic accountAlertsTopic() {
        return TopicBuilder.name(ACCOUNT_ALERTS).build();
    }
}
