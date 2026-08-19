package dev.simd.ledgeflow.service;

import dev.simd.ledgeflow.event.AccountEvent;
import dev.simd.ledgeflow.exception.AccountNotFoundException;
import dev.simd.ledgeflow.exception.InsufficientFundsException;
import dev.simd.ledgeflow.exception.InvalidRequestException;
import dev.simd.ledgeflow.kafka.KafkaEventPublisher;
import dev.simd.ledgeflow.metrics.LedgeFlowMetrics;
import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.model.Transaction;
import dev.simd.ledgeflow.repository.AccountRepository;
import dev.simd.ledgeflow.repository.TransactionRepository;
import dev.simd.ledgeflow.repository.UserRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * CQRS write path. Validates commands against the PostgreSQL read model, then publishes
 * typed events to {@code account.events}. Balances are never set here; the
 * {@link dev.simd.ledgeflow.kafka.EventConsumer} projects them from the event log.
 * <p>
 * Reads from the read model before publishing create a TOCTOU window; {@code withdraw}
 * and {@code transfer} mitigate this with an {@code @Version} bump that serialises
 * concurrent DB commits, surfacing conflicts as HTTP 409.
 */
@Service
public class AccountService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final UserRepository userRepository;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final LedgeFlowMetrics metrics;

    public AccountService(AccountRepository accountRepository,
                          TransactionRepository transactionRepository,
                          UserRepository userRepository,
                          KafkaEventPublisher kafkaEventPublisher,
                          LedgeFlowMetrics metrics) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.userRepository = userRepository;
        this.kafkaEventPublisher = kafkaEventPublisher;
        this.metrics = metrics;
    }

    public Account createAccount(UUID ownerId, String currency) {
        if (!userRepository.existsById(ownerId)) {
            throw new InvalidRequestException("Owner not found: " + ownerId);
        }
        Account account = new Account();
        account.setId(UUID.randomUUID());
        account.setOwnerId(ownerId);
        account.setCurrency(currency);
        account.setBalance(BigDecimal.ZERO);
        account.setCreatedAt(LocalDateTime.now(ZoneOffset.UTC));
        account.setUpdatedAt(LocalDateTime.now(ZoneOffset.UTC));
        Account saved = accountRepository.save(account);
        kafkaEventPublisher.publish(saved.getId().toString(),
                new AccountEvent("AccountCreated", saved.getId(), null, BigDecimal.ZERO, currency,
                        UUID.randomUUID(), Instant.now().toString(), ownerId));
        return saved;
    }

    // No @Transactional: deposit has no balance precondition and makes no write-side save, so there is nothing to version-lock.
    public void deposit(UUID accountId, BigDecimal amount, String currency) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidRequestException("Amount must be positive");
        }
        Account account = getAccount(accountId);
        validateCurrency(account, currency);
        AccountEvent event = new AccountEvent(
                "MoneyDeposited", accountId, null, amount, currency,
                UUID.randomUUID(), Instant.now().toString(), null
        );
        kafkaEventPublisher.publish(accountId.toString(), event);
        metrics.incrementDeposit(amount);
    }

    /**
     * The {@code save()} call does not update the balance; it exists solely to bump the
     * {@code @Version} column so two concurrent withdrawals collide at the DB commit,
     * and the losing request receives HTTP 409 rather than both passing the balance check.
     */
    @Transactional
    public void withdraw(UUID accountId, BigDecimal amount, String currency) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidRequestException("Amount must be positive");
        }
        Account account = getAccount(accountId);
        validateCurrency(account, currency);
        if (account.getBalance().compareTo(amount) < 0) {
            throw new InsufficientFundsException();
        }
        account.setUpdatedAt(LocalDateTime.now(ZoneOffset.UTC));
        accountRepository.save(account);
        AccountEvent event = new AccountEvent(
                "MoneyWithdrawn", accountId, null, amount, currency,
                UUID.randomUUID(), Instant.now().toString(), null
        );
        kafkaEventPublisher.publish(accountId.toString(), event);
        metrics.incrementWithdrawal(amount);
    }

    /**
     * The {@code save()} call does not update the balance; it exists solely to bump the
     * {@code @Version} column on the source account so two concurrent transfers from the
     * same account collide at the DB commit, surfacing the conflict as HTTP 409.
     */
    @Transactional
    public void transfer(UUID fromAccountId, UUID toAccountId, BigDecimal amount, String currency) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidRequestException("Amount must be positive");
        }
        if (fromAccountId.equals(toAccountId)) {
            throw new InvalidRequestException("Source and destination accounts must differ");
        }
        Account source = getAccount(fromAccountId);
        Account destination = getAccount(toAccountId);
        validateCurrency(source, currency);
        validateCurrency(destination, currency);
        if (source.getBalance().compareTo(amount) < 0) {
            throw new InsufficientFundsException();
        }
        source.setUpdatedAt(LocalDateTime.now(ZoneOffset.UTC));
        accountRepository.save(source);
        UUID correlationId = UUID.randomUUID();
        kafkaEventPublisher.publish(fromAccountId.toString(),
                new AccountEvent("TransferCompleted", fromAccountId, toAccountId, amount, currency,
                        correlationId, Instant.now().toString(), null));
        metrics.incrementTransfer(amount);
    }

    public Account getAccount(UUID id) {
        return accountRepository.findById(id)
                .orElseThrow(() -> new AccountNotFoundException(id));
    }

    public Page<Transaction> getAccountHistory(UUID id, Pageable pageable) {
        getAccount(id);
        return transactionRepository.findByAccountId(id, pageable);
    }

    public void assertOwner(UUID accountId, String callerUsername) {
        Account account = getAccount(accountId);
        boolean isOwner = userRepository.findByUsername(callerUsername)
                .map(u -> u.getId().equals(account.getOwnerId()))
                .orElse(false);
        if (!isOwner) {
            throw new AccessDeniedException("Account does not belong to the caller");
        }
    }

    private void validateCurrency(Account account, String currency) {
        if (!account.getCurrency().equals(currency)) {
            throw new InvalidRequestException(
                    "Currency mismatch: account uses " + account.getCurrency()
                    + " but request specified " + currency);
        }
    }
}
