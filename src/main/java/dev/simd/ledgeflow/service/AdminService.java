package dev.simd.ledgeflow.service;

import dev.simd.ledgeflow.config.KafkaTopicConfig;
import dev.simd.ledgeflow.kafka.EventConsumer;
import dev.simd.ledgeflow.repository.AccountRepository;
import dev.simd.ledgeflow.repository.ProcessedEventRepository;
import dev.simd.ledgeflow.repository.TransactionRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Service
public class AdminService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final EventConsumer eventConsumer;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public AdminService(AccountRepository accountRepository,
                        TransactionRepository transactionRepository,
                        ProcessedEventRepository processedEventRepository,
                        EventConsumer eventConsumer) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.processedEventRepository = processedEventRepository;
        this.eventConsumer = eventConsumer;
    }

    public void rebuild() {
        transactionRepository.deleteAll();
        processedEventRepository.deleteAll();
        accountRepository.deleteAll();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledgeflow-rebuild-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(KafkaTopicConfig.ACCOUNT_EVENTS, 0);
            consumer.assign(Collections.singletonList(partition));
            consumer.seekToBeginning(Collections.singletonList(partition));

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(partition));
            long endOffset = endOffsets.getOrDefault(partition, 0L);

            if (endOffset == 0) return;

            while (consumer.position(partition) < endOffset) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(record -> eventConsumer.consume(record.value()));
            }
        }
    }
}
