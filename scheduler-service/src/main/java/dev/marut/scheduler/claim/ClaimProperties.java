package dev.marut.scheduler.claim;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler.claim")
public record ClaimProperties(long pollIntervalMs, int batchSize) { }
