package dev.simd.ledgeflow;

import dev.simd.ledgeflow.exception.AccountNotFoundException;
import dev.simd.ledgeflow.exception.InsufficientFundsException;
import dev.simd.ledgeflow.exception.InvalidRequestException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import dev.simd.ledgeflow.kafka.KafkaEventPublisher;
import dev.simd.ledgeflow.metrics.LedgeFlowMetrics;
import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.repository.AccountRepository;
import dev.simd.ledgeflow.repository.TransactionRepository;
import dev.simd.ledgeflow.service.AccountService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccountServiceTest {

    @Mock AccountRepository accountRepository;
    @Mock TransactionRepository transactionRepository;
    @Mock KafkaEventPublisher kafkaEventPublisher;
    @Mock LedgeFlowMetrics metrics;

    AccountService accountService;

    @BeforeEach
    void setUp() {
        accountService = new AccountService(accountRepository, transactionRepository, kafkaEventPublisher, metrics);
    }

    // --- deposit ---

    @Test
    void deposit_throwsInvalidRequest_whenAmountIsZero() {
        assertThatThrownBy(() -> accountService.deposit(UUID.randomUUID(), BigDecimal.ZERO, "EUR"))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void deposit_throwsInvalidRequest_whenAmountIsNegative() {
        assertThatThrownBy(() -> accountService.deposit(UUID.randomUUID(), new BigDecimal("-1"), "EUR"))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void deposit_throwsAccountNotFound_whenAccountDoesNotExist() {
        UUID id = UUID.randomUUID();
        when(accountRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> accountService.deposit(id, new BigDecimal("50"), "EUR"))
                .isInstanceOf(AccountNotFoundException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void deposit_publishesEvent_whenValid() {
        UUID id = UUID.randomUUID();
        when(accountRepository.findById(id)).thenReturn(Optional.of(accountWithBalance(id, "0.00")));

        accountService.deposit(id, new BigDecimal("100.00"), "EUR");

        verify(kafkaEventPublisher).publish(eq(id.toString()), any());
    }

    // --- withdraw ---

    @Test
    void withdraw_throwsInvalidRequest_whenAmountIsZero() {
        assertThatThrownBy(() -> accountService.withdraw(UUID.randomUUID(), BigDecimal.ZERO, "EUR"))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void withdraw_throwsAccountNotFound_whenAccountDoesNotExist() {
        UUID id = UUID.randomUUID();
        when(accountRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> accountService.withdraw(id, new BigDecimal("50"), "EUR"))
                .isInstanceOf(AccountNotFoundException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void withdraw_throwsInsufficientFunds_whenBalanceTooLow() {
        UUID id = UUID.randomUUID();
        when(accountRepository.findById(id)).thenReturn(Optional.of(accountWithBalance(id, "30.00")));

        assertThatThrownBy(() -> accountService.withdraw(id, new BigDecimal("50.00"), "EUR"))
                .isInstanceOf(InsufficientFundsException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void withdraw_publishesEvent_whenValid() {
        UUID id = UUID.randomUUID();
        when(accountRepository.findById(id)).thenReturn(Optional.of(accountWithBalance(id, "100.00")));

        accountService.withdraw(id, new BigDecimal("40.00"), "EUR");

        verify(kafkaEventPublisher).publish(eq(id.toString()), any());
    }

    // --- transfer ---

    @Test
    void transfer_throwsInvalidRequest_whenAmountIsZero() {
        assertThatThrownBy(() ->
                accountService.transfer(UUID.randomUUID(), UUID.randomUUID(), BigDecimal.ZERO, "EUR"))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void transfer_throwsInvalidRequest_whenSourceAndDestinationAreTheSame() {
        UUID id = UUID.randomUUID();
        assertThatThrownBy(() ->
                accountService.transfer(id, id, new BigDecimal("50"), "EUR"))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void transfer_throwsAccountNotFound_whenSourceDoesNotExist() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        when(accountRepository.findById(from)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> accountService.transfer(from, to, new BigDecimal("50"), "EUR"))
                .isInstanceOf(AccountNotFoundException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void transfer_throwsAccountNotFound_whenDestinationDoesNotExist() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        when(accountRepository.findById(from)).thenReturn(Optional.of(accountWithBalance(from, "200.00")));
        when(accountRepository.findById(to)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> accountService.transfer(from, to, new BigDecimal("50"), "EUR"))
                .isInstanceOf(AccountNotFoundException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void transfer_throwsInsufficientFunds_whenSourceBalanceTooLow() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        when(accountRepository.findById(from)).thenReturn(Optional.of(accountWithBalance(from, "10.00")));
        when(accountRepository.findById(to)).thenReturn(Optional.of(accountWithBalance(to, "0.00")));

        assertThatThrownBy(() -> accountService.transfer(from, to, new BigDecimal("50.00"), "EUR"))
                .isInstanceOf(InsufficientFundsException.class);
        verifyNoInteractions(kafkaEventPublisher);
    }

    @Test
    void transfer_publishesTwoEvents_whenValid() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        when(accountRepository.findById(from)).thenReturn(Optional.of(accountWithBalance(from, "200.00")));
        when(accountRepository.findById(to)).thenReturn(Optional.of(accountWithBalance(to, "0.00")));

        accountService.transfer(from, to, new BigDecimal("50.00"), "EUR");

        verify(kafkaEventPublisher, times(2)).publish(eq(from.toString()), any());
    }

    // --- optimistic locking ---

    @Test
    void withdraw_propagatesOptimisticLock_whenConcurrentModification() {
        UUID id = UUID.randomUUID();
        when(accountRepository.findById(id)).thenReturn(Optional.of(accountWithBalance(id, "100.00")));
        when(accountRepository.save(any())).thenThrow(new ObjectOptimisticLockingFailureException(Account.class, id));

        assertThatThrownBy(() -> accountService.withdraw(id, new BigDecimal("50.00"), "EUR"))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);
    }

    @Test
    void transfer_propagatesOptimisticLock_whenConcurrentModification() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        when(accountRepository.findById(from)).thenReturn(Optional.of(accountWithBalance(from, "200.00")));
        when(accountRepository.findById(to)).thenReturn(Optional.of(accountWithBalance(to, "0.00")));
        when(accountRepository.save(any())).thenThrow(new ObjectOptimisticLockingFailureException(Account.class, from));

        assertThatThrownBy(() -> accountService.transfer(from, to, new BigDecimal("50.00"), "EUR"))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);
    }

    // --- helpers ---

    private Account accountWithBalance(UUID id, String balance) {
        return new Account(id, UUID.randomUUID(), "EUR", new BigDecimal(balance),
                LocalDateTime.now(), LocalDateTime.now(), null);
    }
}
