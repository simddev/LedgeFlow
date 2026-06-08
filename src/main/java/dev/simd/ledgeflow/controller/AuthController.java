package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.service.AuthService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    record AuthRequest(@NotBlank String username, @NotBlank String password) {}

    record TokenResponse(String token) {}

    @PostMapping("/register")
    public TokenResponse register(@Valid @RequestBody AuthRequest request) {
        return new TokenResponse(authService.register(request.username(), request.password()));
    }

    @PostMapping("/login")
    public TokenResponse login(@Valid @RequestBody AuthRequest request) {
        return new TokenResponse(authService.login(request.username(), request.password()));
    }
}
