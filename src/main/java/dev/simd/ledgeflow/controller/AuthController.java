package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.service.AuthService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    record RegisterRequest(@NotBlank String username,
                           @NotBlank @Size(min = 8) String password) {}

    record LoginRequest(@NotBlank String username,
                        @NotBlank String password) {}

    record TokenResponse(String token) {}

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public TokenResponse register(@Valid @RequestBody RegisterRequest request) {
        return new TokenResponse(authService.register(request.username(), request.password()));
    }

    @PostMapping("/login")
    public TokenResponse login(@Valid @RequestBody LoginRequest request) {
        return new TokenResponse(authService.login(request.username(), request.password()));
    }
}
