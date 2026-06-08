package dev.simd.ledgeflow.exception;

public class UsernameAlreadyTakenException extends RuntimeException {

    public UsernameAlreadyTakenException() {
        super("Username already taken");
    }
}
