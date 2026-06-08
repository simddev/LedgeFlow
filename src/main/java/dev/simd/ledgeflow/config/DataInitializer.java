package dev.simd.ledgeflow.config;

import dev.simd.ledgeflow.model.User;
import dev.simd.ledgeflow.repository.UserRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class DataInitializer implements ApplicationRunner {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public DataInitializer(UserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (userRepository.findByUsername("admin").isEmpty()) {
            User admin = new User();
            admin.setId(UUID.randomUUID());
            admin.setUsername("admin");
            admin.setPassword(passwordEncoder.encode("admin"));
            admin.setRole("ADMIN");
            admin.setCreatedAt(LocalDateTime.now());
            userRepository.save(admin);
        }
    }
}
