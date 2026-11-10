package dev.marut.worker.handlers;

/** Throw from a handler when retrying cannot help (invalid payload, 4xx from a downstream API). */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) { super(message); }
    public NonRetryableException(String message, Throwable cause) { super(message, cause); }
}
