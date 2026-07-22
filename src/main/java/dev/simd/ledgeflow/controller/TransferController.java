package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.service.AccountService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
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

    record TransferRequest(@NotNull UUID fromAccountId, @NotNull UUID toAccountId,
                           @NotNull @Positive @Digits(integer = 15, fraction = 4) BigDecimal amount,
                           @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {}

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void transfer(@Valid @RequestBody TransferRequest request) {
        accountService.transfer(request.fromAccountId(), request.toAccountId(), request.amount(), request.currency());
    }
}
