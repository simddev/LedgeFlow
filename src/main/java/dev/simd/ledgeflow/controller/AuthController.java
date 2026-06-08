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

    @PostMapping("/register")
    public String register(@Valid @RequestBody AuthRequest request) {
        return authService.register(request.username(), request.password());
    }

    @PostMapping("/login")
    public String login(@Valid @RequestBody AuthRequest request) {
        return authService.login(request.username(), request.password());
    }
}
