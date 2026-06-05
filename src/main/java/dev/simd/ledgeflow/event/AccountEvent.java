package dev.simd.ledgeflow.event;

import lombok.*;

import java.math.BigDecimal;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AccountEvent {

    private String type;
    private UUID accountId;
    private BigDecimal amount;
    private String currency;
    private UUID correlationId;
    private String timestamp;
}
