package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.model.Account;
import dev.simd.ledgeflow.service.AccountService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

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
}
