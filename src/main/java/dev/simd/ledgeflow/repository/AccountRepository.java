package dev.simd.ledgeflow.repository;

import dev.simd.ledgeflow.model.Account;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AccountRepository extends JpaRepository<Account, UUID> {
}
