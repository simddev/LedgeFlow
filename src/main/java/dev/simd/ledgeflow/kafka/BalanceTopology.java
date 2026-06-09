package dev.simd.ledgeflow.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.simd.ledgeflow.config.KafkaTopicConfig;
import dev.simd.ledgeflow.event.AccountEvent;
import dev.simd.ledgeflow.metrics.LedgeFlowMetrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Collections;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class BalanceTopology {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ObjectMapper objectMapper;
    private final LedgeFlowMetrics metrics;

    public BalanceTopology(ObjectMapper objectMapper, LedgeFlowMetrics metrics) {
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ledgeflow-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Materialises per-account balances into the {@code balance-store} state store.
     * This store is the authoritative balance and the intended write-path source via
     * Kafka Streams interactive queries, eliminating the need to read from the PostgreSQL
     * read model on the write path.
     */
    @Bean
    public KTable<String, String> balanceTable(StreamsBuilder builder) {
        KStream<String, String> events = builder.stream(KafkaTopicConfig.ACCOUNT_EVENTS);

        // TransferCompleted is keyed by fromAccountId but must update both accounts.
        // flatMap expands it into two records: one per affected account.
        KStream<String, String> expanded = events
                .filter((key, value) -> isBalanceEvent(value))
                .flatMap((key, value) -> expandTransfer(key, value));

        return expanded
                .groupByKey()
                .aggregate(
                        () -> BigDecimal.ZERO.toPlainString(),
                        (accountId, eventJson, currentBalance) -> applyEvent(accountId, eventJson, currentBalance),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("balance-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );
    }

    @Bean
    public KStream<String, String> alertsStream(StreamsBuilder builder) {
        KStream<String, String> events = builder.stream(KafkaTopicConfig.ACCOUNT_EVENTS);
        KStream<String, String> alerts = events.filter((key, value) -> isLargeTransaction(value));
        alerts.peek((key, value) -> metrics.incrementAlert()).to(KafkaTopicConfig.ACCOUNT_ALERTS);
        return alerts;
    }

    private boolean isLargeTransaction(String value) {
        if (value == null) return false;
        try {
            AccountEvent event = objectMapper.readValue(value, AccountEvent.class);
            return event.getAmount() != null
                    && event.getAmount().compareTo(new BigDecimal("10000")) > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isBalanceEvent(String value) {
        if (value == null) return false;
        try {
            AccountEvent event = objectMapper.readValue(value, AccountEvent.class);
            String type = event.getType();
            return "MoneyDeposited".equals(type)
                    || "MoneyWithdrawn".equals(type)
                    || "TransferCompleted".equals(type);
        } catch (Exception e) {
            return false;
        }
    }

    // TransferCompleted is keyed by fromAccountId, so without fan-out the receiver's balance store entry is never touched.
    private java.util.List<KeyValue<String, String>> expandTransfer(String key, String value) {
        try {
            AccountEvent event = objectMapper.readValue(value, AccountEvent.class);
            if (!"TransferCompleted".equals(event.getType()) || event.getToAccountId() == null) {
                return Collections.singletonList(KeyValue.pair(key, value));
            }
            // emit one record per affected account so both balances are updated
            java.util.List<KeyValue<String, String>> records = new ArrayList<>();
            records.add(KeyValue.pair(event.getAccountId().toString(), value));
            records.add(KeyValue.pair(event.getToAccountId().toString(), value));
            return records;
        } catch (Exception e) {
            return Collections.singletonList(KeyValue.pair(key, value));
        }
    }

    private String applyEvent(String accountId, String eventJson, String currentBalance) {
        try {
            AccountEvent event = objectMapper.readValue(eventJson, AccountEvent.class);
            BigDecimal balance = new BigDecimal(currentBalance);
            balance = switch (event.getType()) {
                case "MoneyDeposited" -> balance.add(event.getAmount());
                case "MoneyWithdrawn" -> balance.subtract(event.getAmount());
                case "TransferCompleted" -> accountId.equals(event.getAccountId().toString())
                        ? balance.subtract(event.getAmount())   // sender
                        : balance.add(event.getAmount());        // receiver
                default -> balance;
            };
            return balance.toPlainString();
        } catch (Exception e) {
            return currentBalance;
        }
    }
}
