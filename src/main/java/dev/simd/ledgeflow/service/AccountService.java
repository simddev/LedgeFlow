package dev.simd.ledgeflow.service;

import dev.simd.ledgeflow.event.AccountEvent;
import dev.simd.ledgeflow.kafka.KafkaEventPublisher;
import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.model.Transaction;
import dev.simd.ledgeflow.repository.AccountRepository;
import dev.simd.ledgeflow.repository.TransactionRepository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class AccountService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final KafkaEventPublisher kafkaEventPublisher;

    public AccountService(AccountRepository accountRepository,
                          TransactionRepository transactionRepository,
                          KafkaEventPublisher kafkaEventPublisher) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    public Account createAccount(UUID ownerId, String currency) {
        Account account = new Account();
        account.setId(UUID.randomUUID());
        account.setOwnerId(ownerId);
        account.setCurrency(currency);
        account.setBalance(BigDecimal.ZERO);
        account.setCreatedAt(LocalDateTime.now());
        account.setUpdatedAt(LocalDateTime.now());
        return accountRepository.save(account);
    }

    public void deposit(UUID accountId, BigDecimal amount, String currency) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new RuntimeException("Amount must be positive");
        }
        getAccount(accountId);
        AccountEvent event = new AccountEvent(
                "MoneyDeposited", accountId, null, amount, currency,
                UUID.randomUUID(), LocalDateTime.now().toString()
        );
        kafkaEventPublisher.publish(accountId.toString(), event);
    }

    public void withdraw(UUID accountId, BigDecimal amount, String currency) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new RuntimeException("Amount must be positive");
        }
        Account account = getAccount(accountId);
        if (account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient funds");
        }
        AccountEvent event = new AccountEvent(
                "MoneyWithdrawn", accountId, null, amount, currency,
                UUID.randomUUID(), LocalDateTime.now().toString()
        );
        kafkaEventPublisher.publish(accountId.toString(), event);
    }

    public Account getAccount(UUID id) {
        return accountRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Account not found: " + id));
    }

    public List<Transaction> getAccountHistory(UUID id) {
        return transactionRepository.findByAccountIdOrderByOccurredAtDesc(id);
    }
}
