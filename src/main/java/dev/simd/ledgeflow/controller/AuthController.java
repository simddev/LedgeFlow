package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.service.AuthService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    record AuthRequest(String username, String password) {}

    @PostMapping("/register")
    public String register(@RequestBody AuthRequest request) {
        return authService.register(request.username(), request.password());
    }

    @PostMapping("/login")
    public String login(@RequestBody AuthRequest request) {
        return authService.login(request.username(), request.password());
    }
}
