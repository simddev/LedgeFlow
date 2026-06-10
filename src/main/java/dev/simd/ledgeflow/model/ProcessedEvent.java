package dev.simd.ledgeflow.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "correlation_id")
    private UUID correlationId;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;
}
