package dev.simd.ledgeflow.service;

import dev.simd.ledgeflow.exception.InvalidCredentialsException;
import dev.simd.ledgeflow.exception.UsernameAlreadyTakenException;
import dev.simd.ledgeflow.model.User;
import dev.simd.ledgeflow.repository.UserRepository;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;

    public AuthService(UserRepository userRepository, PasswordEncoder passwordEncoder, JwtService jwtService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
    }

    public String register(String username, String password) {
        if (userRepository.findByUsername(username).isPresent()) {
            throw new UsernameAlreadyTakenException();
        }
        User user = new User();
        user.setId(UUID.randomUUID());
        user.setUsername(username);
        user.setPassword(passwordEncoder.encode(password));
        user.setRole("USER");
        user.setCreatedAt(LocalDateTime.now());
        userRepository.save(user);
        return jwtService.generateToken(username, user.getRole());
    }

    public String login(String username, String password) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(InvalidCredentialsException::new);
        if (!passwordEncoder.matches(password, user.getPassword())) {
            throw new InvalidCredentialsException();
        }
        return jwtService.generateToken(username, user.getRole());
    }
}
