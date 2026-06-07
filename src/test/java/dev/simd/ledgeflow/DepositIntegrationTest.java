package dev.simd.ledgeflow;

import dev.simd.ledgeflow.event.AccountEvent;
import dev.simd.ledgeflow.kafka.KafkaEventPublisher;
import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.repository.AccountRepository;
import dev.simd.ledgeflow.service.AccountService;
import dev.simd.ledgeflow.service.AdminService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext
class DepositIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .load()
                .migrate();

        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic("account.events", 1, (short) 1),
                    new NewTopic("account.alerts", 1, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Kafka topics", e);
        }
    }

    @Autowired
    private AccountService accountService;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private KafkaEventPublisher kafkaEventPublisher;

    @Autowired
    private AdminService adminService;

    @Test
    void deposit_updatesBalance_inReadModel() {
        Account account = accountService.createAccount(UUID.randomUUID(), "EUR");
        accountService.deposit(account.getId(), new BigDecimal("100.00"), "EUR");

        await().atMost(15, SECONDS).untilAsserted(() -> {
            Account updated = accountRepository.findById(account.getId()).orElseThrow();
            assertThat(updated.getBalance()).isEqualByComparingTo("100.00");
        });
    }

    @Test
    void withdrawal_updatesBalance_inReadModel() {
        Account account = accountService.createAccount(UUID.randomUUID(), "EUR");
        account.setBalance(new BigDecimal("200.00"));
        accountRepository.save(account);

        accountService.withdraw(account.getId(), new BigDecimal("50.00"), "EUR");

        await().atMost(15, SECONDS).untilAsserted(() -> {
            Account updated = accountRepository.findById(account.getId()).orElseThrow();
            assertThat(updated.getBalance()).isEqualByComparingTo("150.00");
        });
    }

    @Test
    void deposit_isIdempotent_duplicateEventIgnored() {
        Account account = accountService.createAccount(UUID.randomUUID(), "EUR");

        UUID correlationId = UUID.randomUUID();
        AccountEvent event = new AccountEvent(
                "MoneyDeposited", account.getId(), null,
                new BigDecimal("100.00"), "EUR", correlationId, LocalDateTime.now().toString());

        kafkaEventPublisher.publish(account.getId().toString(), event);
        await().atMost(15, SECONDS).untilAsserted(() -> {
            Account updated = accountRepository.findById(account.getId()).orElseThrow();
            assertThat(updated.getBalance()).isEqualByComparingTo("100.00");
        });

        // send exact same event again — consumer must ignore it
        kafkaEventPublisher.publish(account.getId().toString(), event);
        await().during(4, SECONDS).atMost(6, SECONDS).untilAsserted(() -> {
            Account updated = accountRepository.findById(account.getId()).orElseThrow();
            assertThat(updated.getBalance()).isEqualByComparingTo("100.00");
        });
    }

    @Test
    void rebuild_restoresReadModel_fromKafkaEventLog() {
        Account account = accountService.createAccount(UUID.randomUUID(), "EUR");
        accountService.deposit(account.getId(), new BigDecimal("100.00"), "EUR");

        await().atMost(15, SECONDS).untilAsserted(() -> {
            Account updated = accountRepository.findById(account.getId()).orElseThrow();
            assertThat(updated.getBalance()).isEqualByComparingTo("100.00");
        });

        adminService.rebuild();

        Account rebuilt = accountRepository.findById(account.getId()).orElseThrow();
        assertThat(rebuilt.getBalance()).isEqualByComparingTo("100.00");
    }

    @Test
    void transfer_updatesBalances_inReadModel() {
        Account source = accountService.createAccount(UUID.randomUUID(), "EUR");
        source.setBalance(new BigDecimal("300.00"));
        accountRepository.save(source);

        Account destination = accountService.createAccount(UUID.randomUUID(), "EUR");

        accountService.transfer(source.getId(), destination.getId(), new BigDecimal("75.00"), "EUR");

        await().atMost(15, SECONDS).untilAsserted(() -> {
            Account updatedSource = accountRepository.findById(source.getId()).orElseThrow();
            Account updatedDest = accountRepository.findById(destination.getId()).orElseThrow();
            assertThat(updatedSource.getBalance()).isEqualByComparingTo("225.00");
            assertThat(updatedDest.getBalance()).isEqualByComparingTo("75.00");
        });
    }
}
