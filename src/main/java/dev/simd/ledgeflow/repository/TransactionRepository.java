package dev.simd.ledgeflow.repository;

import dev.simd.ledgeflow.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    List<Transaction> findByAccountIdOrderByOccurredAtDesc(UUID accountId);
}
