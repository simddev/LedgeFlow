package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.service.AccountService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/transfers")
public class TransferController {

    private final AccountService accountService;

    public TransferController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void transfer(@RequestParam UUID fromAccountId,
                         @RequestParam UUID toAccountId,
                         @RequestParam BigDecimal amount,
                         @RequestParam String currency) {
        accountService.transfer(fromAccountId, toAccountId, amount, currency);
    }
}
