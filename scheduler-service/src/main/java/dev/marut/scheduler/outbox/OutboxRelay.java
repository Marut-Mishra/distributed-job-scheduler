package dev.marut.scheduler.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Relays outbox rows to Kafka. Guarantee: at-least-once.
 * If the process dies after Kafka acks but before the DB commit, the batch is re-sent on the
 * next tick → consumers can see duplicates. That is deliberate; exactly-once is pushed to the
 * consumer side (idempotent state transitions + idempotent handlers).
 */
@Component
@ConditionalOnProperty(name = "scheduler.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka,
                       TransactionTemplate tx, MeterRegistry metrics) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.tx = tx;
        // Lag of the relay — the first thing to alert on if Kafka is down.
        Gauge.builder("scheduler.outbox.pending", outbox, OutboxRepository::countByPublishedAtIsNull)
                .register(metrics);
    }

    @Scheduled(fixedDelayString = "${scheduler.outbox.poll-interval-ms:500}")
    public void relay() {
        try {
            Integer sent;
            do {
                sent = tx.execute(status -> publishBatch());
            } while (sent != null && sent == BATCH);
        } catch (Exception e) {
            log.warn("Outbox relay failed, will retry: {}", e.getMessage());
        }
    }

    private int publishBatch() {
        List<OutboxEvent> batch = outbox.lockUnpublished(BATCH);
        for (OutboxEvent e : batch) {
            e.recordAttempt();
            try {
                // Synchronous send: we only mark published after the broker has acked (acks=all).
                kafka.send(e.getTopic(), e.getMessageKey(), e.getPayload()).get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                // Abort the batch; the tx rolls back and these rows stay unpublished.
                throw new IllegalStateException("Kafka publish failed for outbox " + e.getId(), ex);
            }
            e.markPublished();
        }
        return batch.size();
    }
}
