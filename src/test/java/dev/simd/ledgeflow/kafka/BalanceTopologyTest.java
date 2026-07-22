package dev.simd.ledgeflow.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.simd.ledgeflow.config.KafkaTopicConfig;
import dev.simd.ledgeflow.event.AccountEvent;
import dev.simd.ledgeflow.metrics.LedgeFlowMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BalanceTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> eventsTopic;
    private TestOutputTopic<String, String> alertsTopic;
    private ObjectMapper objectMapper;

    @TempDir
    File stateDir;

    @BeforeEach
    void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        LedgeFlowMetrics metrics = new LedgeFlowMetrics(new SimpleMeterRegistry());

        BalanceTopology topology = new BalanceTopology("dummy:9092", objectMapper, metrics);

        StreamsBuilder builder = new StreamsBuilder();
        topology.balanceTable(builder);
        topology.alertsStream(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.StringSerde.class);

        driver = new TopologyTestDriver(builder.build(), props);
        eventsTopic = driver.createInputTopic(KafkaTopicConfig.ACCOUNT_EVENTS,
                new StringSerializer(), new StringSerializer());
        alertsTopic = driver.createOutputTopic(KafkaTopicConfig.ACCOUNT_ALERTS,
                new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void deposit_increases_balance_in_store() throws Exception {
        UUID accountId = UUID.randomUUID();
        send(deposit(accountId, "100.00"));

        ReadOnlyKeyValueStore<String, String> store = driver.getKeyValueStore("balance-store");
        assertThat(new BigDecimal(store.get(accountId.toString()))).isEqualByComparingTo("100.00");
    }

    @Test
    void withdrawal_decreases_balance_in_store() throws Exception {
        UUID accountId = UUID.randomUUID();
        send(deposit(accountId, "200.00"));
        send(withdrawal(accountId, "75.00"));

        ReadOnlyKeyValueStore<String, String> store = driver.getKeyValueStore("balance-store");
        assertThat(new BigDecimal(store.get(accountId.toString()))).isEqualByComparingTo("125.00");
    }

    @Test
    void transfer_updates_both_account_balances_in_store() throws Exception {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        send(deposit(from, "300.00"));
        send(transfer(from, to, "100.00"));

        ReadOnlyKeyValueStore<String, String> store = driver.getKeyValueStore("balance-store");
        assertThat(new BigDecimal(store.get(from.toString()))).isEqualByComparingTo("200.00");
        assertThat(new BigDecimal(store.get(to.toString()))).isEqualByComparingTo("100.00");
    }

    @Test
    void large_transaction_routes_to_alerts_topic() throws Exception {
        UUID accountId = UUID.randomUUID();
        send(deposit(accountId, "50000.00"));

        assertThat(alertsTopic.isEmpty()).isFalse();
    }

    @Test
    void small_transaction_does_not_route_to_alerts_topic() throws Exception {
        UUID accountId = UUID.randomUUID();
        send(deposit(accountId, "50.00"));

        assertThat(alertsTopic.isEmpty()).isTrue();
    }

    @Test
    void account_created_event_does_not_affect_balance_store() throws Exception {
        UUID accountId = UUID.randomUUID();
        AccountEvent event = new AccountEvent("AccountCreated", accountId, null,
                BigDecimal.ZERO, "EUR", UUID.randomUUID(), Instant.now().toString(), UUID.randomUUID());
        eventsTopic.pipeInput(accountId.toString(), objectMapper.writeValueAsString(event));

        ReadOnlyKeyValueStore<String, String> store = driver.getKeyValueStore("balance-store");
        assertThat(store.get(accountId.toString())).isNull();
    }

    private void send(AccountEvent event) throws Exception {
        eventsTopic.pipeInput(event.getAccountId().toString(), objectMapper.writeValueAsString(event));
    }

    private AccountEvent deposit(UUID accountId, String amount) {
        return new AccountEvent("MoneyDeposited", accountId, null,
                new BigDecimal(amount), "EUR", UUID.randomUUID(), Instant.now().toString(), null);
    }

    private AccountEvent withdrawal(UUID accountId, String amount) {
        return new AccountEvent("MoneyWithdrawn", accountId, null,
                new BigDecimal(amount), "EUR", UUID.randomUUID(), Instant.now().toString(), null);
    }

    private AccountEvent transfer(UUID from, UUID to, String amount) {
        return new AccountEvent("TransferCompleted", from, to,
                new BigDecimal(amount), "EUR", UUID.randomUUID(), Instant.now().toString(), null);
    }
}
