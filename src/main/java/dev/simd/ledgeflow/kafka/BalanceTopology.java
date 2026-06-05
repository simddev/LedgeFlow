package dev.simd.ledgeflow.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.simd.ledgeflow.config.KafkaTopicConfig;
import dev.simd.ledgeflow.event.AccountEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
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

    public BalanceTopology(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
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

    @Bean
    public KTable<String, String> balanceTable(StreamsBuilder builder) {
        KStream<String, String> events = builder.stream(KafkaTopicConfig.ACCOUNT_EVENTS);

        return events
                .filter((key, value) -> isBalanceEvent(value))
                .groupByKey()
                .aggregate(
                        () -> BigDecimal.ZERO.toPlainString(),
                        (accountId, eventJson, currentBalance) -> applyEvent(eventJson, currentBalance),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("balance-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );
    }

    @Bean
    public KTable<Windowed<String>, Long> velocityTable(StreamsBuilder builder) {
        KStream<String, String> events = builder.stream(KafkaTopicConfig.ACCOUNT_EVENTS);

        return events
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("velocity-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));
    }

    private boolean isBalanceEvent(String value) {
        if (value == null) return false;
        return value.contains("MoneyDeposited")
                || value.contains("MoneyWithdrawn")
                || value.contains("TransferCompleted");
    }

    private String applyEvent(String eventJson, String currentBalance) {
        try {
            AccountEvent event = objectMapper.readValue(eventJson, AccountEvent.class);
            BigDecimal balance = new BigDecimal(currentBalance);
            switch (event.getType()) {
                case "MoneyDeposited"    -> balance = balance.add(event.getAmount());
                case "MoneyWithdrawn"    -> balance = balance.subtract(event.getAmount());
                case "TransferCompleted" -> balance = balance.subtract(event.getAmount());
            }
            return balance.toPlainString();
        } catch (Exception e) {
            return currentBalance;
        }
    }
}
