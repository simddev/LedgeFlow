package dev.simd.ledgeflow.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.simd.ledgeflow.event.AccountEvent;
import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.model.ProcessedEvent;
import dev.simd.ledgeflow.model.Transaction;
import dev.simd.ledgeflow.repository.AccountRepository;
import dev.simd.ledgeflow.repository.ProcessedEventRepository;
import dev.simd.ledgeflow.repository.TransactionRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class EventConsumer {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public EventConsumer(AccountRepository accountRepository,
                         TransactionRepository transactionRepository,
                         ProcessedEventRepository processedEventRepository,
                         ObjectMapper objectMapper) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Idempotent: each event is deduplicated by {@code correlationId} via the
     * {@code processed_events} table, so redelivery and admin rebuild replays are both safe.
     */
    @KafkaListener(topics = "${kafka.topic.account-events:account.events}",
                   groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consume(String message) {
        AccountEvent event;
        try {
            event = objectMapper.readValue(message, AccountEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize event", e);
        }

        if (processedEventRepository.existsById(event.getCorrelationId())) {
            return;
        }

        boolean handled = switch (event.getType()) {
            case "AccountCreated"    -> { createAccount(event);     yield true; }
            case "MoneyDeposited"    -> { processDeposit(event);    yield true; }
            case "MoneyWithdrawn"    -> { processWithdrawal(event); yield true; }
            case "TransferCompleted" -> { processTransfer(event);   yield true; }
            default                  -> false;
        };

        if (handled) {
            ProcessedEvent processed = new ProcessedEvent();
            processed.setCorrelationId(event.getCorrelationId());
            processed.setProcessedAt(LocalDateTime.now());
            processedEventRepository.save(processed);
        }
    }

    private void createAccount(AccountEvent event) {
        // AccountService.createAccount() already saves directly to the DB, so the event arrives after the row exists.
        // Skip on normal delivery; on rebuild the row is gone and this insert is required.
        if (accountRepository.existsById(event.getAccountId())) {
            return;
        }
        Account account = new Account();
        account.setId(event.getAccountId());
        account.setOwnerId(event.getOwnerId());
        account.setCurrency(event.getCurrency());
        account.setBalance(BigDecimal.ZERO);
        account.setCreatedAt(LocalDateTime.now());
        account.setUpdatedAt(LocalDateTime.now());
        accountRepository.save(account);
    }

    private void processDeposit(AccountEvent event) {
        Account account = getAccount(event.getAccountId());
        account.setBalance(account.getBalance().add(event.getAmount()));
        account.setUpdatedAt(LocalDateTime.now());
        accountRepository.save(account);
        insertTransaction(event.getAccountId(), "DEPOSIT", event.getAmount(), account.getBalance(), event.getCorrelationId());
    }

    private void processWithdrawal(AccountEvent event) {
        Account account = getAccount(event.getAccountId());
        account.setBalance(account.getBalance().subtract(event.getAmount()));
        account.setUpdatedAt(LocalDateTime.now());
        accountRepository.save(account);
        insertTransaction(event.getAccountId(), "WITHDRAWAL", event.getAmount(), account.getBalance(), event.getCorrelationId());
    }

    private void processTransfer(AccountEvent event) {
        Account source = getAccount(event.getAccountId());
        source.setBalance(source.getBalance().subtract(event.getAmount()));
        source.setUpdatedAt(LocalDateTime.now());
        accountRepository.save(source);
        insertTransaction(event.getAccountId(), "TRANSFER_OUT", event.getAmount(), source.getBalance(), event.getCorrelationId());

        Account destination = getAccount(event.getToAccountId());
        destination.setBalance(destination.getBalance().add(event.getAmount()));
        destination.setUpdatedAt(LocalDateTime.now());
        accountRepository.save(destination);
        insertTransaction(event.getToAccountId(), "TRANSFER_IN", event.getAmount(), destination.getBalance(), event.getCorrelationId());
    }

    // Intentionally throws RuntimeException so the @KafkaListener error handler retries rather than silently skipping.
    private Account getAccount(UUID id) {
        return accountRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Account not found: " + id));
    }

    private void insertTransaction(UUID accountId, String type, BigDecimal amount,
                                   BigDecimal balanceAfter, UUID correlationId) {
        Transaction transaction = new Transaction();
        transaction.setId(UUID.randomUUID());
        transaction.setAccountId(accountId);
        transaction.setType(type);
        transaction.setAmount(amount);
        transaction.setBalanceAfter(balanceAfter);
        transaction.setCorrelationId(correlationId);
        transaction.setOccurredAt(LocalDateTime.now());
        transactionRepository.save(transaction);
    }
}
