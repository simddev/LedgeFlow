package dev.simd.ledgeflow.service;

import dev.simd.ledgeflow.config.KafkaTopicConfig;
import dev.simd.ledgeflow.kafka.EventConsumer;
import dev.simd.ledgeflow.metrics.LedgeFlowMetrics;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

@Service
public class AdminService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final EventConsumer eventConsumer;
    private final LedgeFlowMetrics metrics;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public AdminService(AccountRepository accountRepository,
                        TransactionRepository transactionRepository,
                        ProcessedEventRepository processedEventRepository,
                        EventConsumer eventConsumer,
                        LedgeFlowMetrics metrics) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.processedEventRepository = processedEventRepository;
        this.eventConsumer = eventConsumer;
        this.metrics = metrics;
    }

    public void rebuild() {
        metrics.getRebuildTimer().record(() -> doRebuild());
    }

    /**
     * Wipes the entire read model, then replays {@code account.events} from offset 0 using
     * a throwaway consumer group so no committed offset interferes. End offsets are
     * snapshotted before polling begins, ensuring the loop terminates even if new events
     * arrive during the rebuild. Projection logic is intentionally delegated to
     * {@link EventConsumer#consume} so it stays single-sourced.
     */
    private void doRebuild() {
        transactionRepository.deleteAll();
        processedEventRepository.deleteAll();
        accountRepository.deleteAll();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // random group ID so Kafka never has a committed offset for this consumer; always reads from the seekToBeginning below
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledgeflow-rebuild-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(KafkaTopicConfig.ACCOUNT_EVENTS)
                    .stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Set<TopicPartition> remaining = new HashSet<>(endOffsets.keySet());
            remaining.removeIf(tp -> endOffsets.get(tp) == 0);

            if (remaining.isEmpty()) return;

            while (!remaining.isEmpty()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(record -> eventConsumer.consume(record.value()));
                remaining.removeIf(tp -> consumer.position(tp) >= endOffsets.get(tp));
            }
        }
    }
}
