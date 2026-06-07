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

    record TransferRequest(UUID fromAccountId, UUID toAccountId, BigDecimal amount, String currency) {}

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void transfer(@RequestBody TransferRequest request) {
        accountService.transfer(request.fromAccountId(), request.toAccountId(), request.amount(), request.currency());
    }
}
