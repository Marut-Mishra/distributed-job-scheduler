package dev.marut.scheduler.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Appends to the outbox. MANDATORY: must join the caller's transaction, or the guarantee is gone. */
@Component
public class OutboxWriter {

    private final OutboxRepository outbox;
    private final ObjectMapper json;

    public OutboxWriter(OutboxRepository outbox, ObjectMapper json) {
        this.outbox = outbox;
        this.json = json;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String topic, String key, Object message) {
        try {
            outbox.save(OutboxEvent.of(topic, key, json.writeValueAsString(message)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise outbox message", e);
        }
    }
}
