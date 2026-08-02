package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.model.Transaction;
import dev.simd.ledgeflow.service.AccountService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    record CreateAccountRequest(@NotNull UUID ownerId,
                                @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

    record AccountResponse(UUID id, UUID ownerId, String currency, BigDecimal balance,
                           LocalDateTime createdAt, LocalDateTime updatedAt) {
        static AccountResponse from(Account a) {
            return new AccountResponse(a.getId(), a.getOwnerId(), a.getCurrency(),
                    a.getBalance(), a.getCreatedAt(), a.getUpdatedAt());
        }
    }

    record TransactionResponse(UUID id, UUID accountId, String type, BigDecimal amount,
                                BigDecimal balanceAfter, UUID correlationId,
                                LocalDateTime occurredAt) {
        static TransactionResponse from(Transaction t) {
            return new TransactionResponse(t.getId(), t.getAccountId(), t.getType(),
                    t.getAmount(), t.getBalanceAfter(), t.getCorrelationId(), t.getOccurredAt());
        }
    }

    record AmountRequest(@NotNull @Positive @Digits(integer = 15, fraction = 4) BigDecimal amount,
                         @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

    record TransferRequest(@NotNull UUID toAccountId,
                           @NotNull @Positive @Digits(integer = 15, fraction = 4) BigDecimal amount,
                           @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public AccountResponse createAccount(@Valid @RequestBody CreateAccountRequest request) {
        return AccountResponse.from(accountService.createAccount(request.ownerId(), request.currency()));
    }

    @GetMapping("/{id}")
    public AccountResponse getAccount(@PathVariable UUID id) {
        return AccountResponse.from(accountService.getAccount(id));
    }

    @GetMapping("/{id}/history")
    public Page<TransactionResponse> getAccountHistory(
            @PathVariable UUID id,
            @PageableDefault(size = 50, sort = "occurredAt", direction = Sort.Direction.DESC) Pageable pageable) {
        return accountService.getAccountHistory(id, pageable).map(TransactionResponse::from);
    }

    @PostMapping("/{id}/deposit")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deposit(@PathVariable UUID id, @Valid @RequestBody AmountRequest request) {
        accountService.deposit(id, request.amount(), request.currency());
    }

    @PostMapping("/{id}/withdraw")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void withdraw(@PathVariable UUID id, @Valid @RequestBody AmountRequest request) {
        accountService.withdraw(id, request.amount(), request.currency());
    }

    @PostMapping("/{id}/transfer")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void transfer(@PathVariable UUID id, @Valid @RequestBody TransferRequest request) {
        accountService.transfer(id, request.toAccountId(), request.amount(), request.currency());
    }
}
