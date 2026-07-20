package dev.simd.ledgeflow.exception;

public class RebuildInProgressException extends RuntimeException {

    public RebuildInProgressException() {
        super("Rebuild already in progress");
    }
}
