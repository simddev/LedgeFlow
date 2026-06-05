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

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Account createAccount(@RequestParam UUID ownerId, @RequestParam String currency) {
        return accountService.createAccount(ownerId, currency);
    }

    @GetMapping("/{id}")
    public Account getAccount(@PathVariable UUID id) {
        return accountService.getAccount(id);
    }

    @GetMapping("/{id}/history")
    public List<Transaction> getAccountHistory(@PathVariable UUID id) {
        return accountService.getAccountHistory(id);
    }

    @PostMapping("/{id}/deposit")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deposit(@PathVariable UUID id,
                        @RequestParam BigDecimal amount,
                        @RequestParam String currency) {
        accountService.deposit(id, amount, currency);
    }
}
