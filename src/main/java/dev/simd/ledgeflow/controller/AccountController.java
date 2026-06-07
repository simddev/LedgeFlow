package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.model.Transaction;
import dev.simd.ledgeflow.service.AccountService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    record CreateAccountRequest(UUID ownerId, String currency) {}

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Account createAccount(@RequestBody CreateAccountRequest request) {
        return accountService.createAccount(request.ownerId(), request.currency());
    }

    @GetMapping("/{id}")
    public Account getAccount(@PathVariable UUID id) {
        return accountService.getAccount(id);
    }

    @GetMapping("/{id}/history")
    public List<Transaction> getAccountHistory(@PathVariable UUID id) {
        return accountService.getAccountHistory(id);
    }

    record AmountRequest(BigDecimal amount, String currency) {}

    @PostMapping("/{id}/deposit")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deposit(@PathVariable UUID id, @RequestBody AmountRequest request) {
        accountService.deposit(id, request.amount(), request.currency());
    }

    @PostMapping("/{id}/withdraw")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void withdraw(@PathVariable UUID id, @RequestBody AmountRequest request) {
        accountService.withdraw(id, request.amount(), request.currency());
    }
}
