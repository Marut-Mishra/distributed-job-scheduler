package dev.marut.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.marut.scheduler.contracts.RunDispatch;
import dev.marut.scheduler.contracts.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DispatchListener {

    private final ObjectMapper json;
    private final RunExecutor executor;

    public DispatchListener(ObjectMapper json, RunExecutor executor) {
        this.json = json;
        this.executor = executor;
    }

    /**
     * The offset is committed only after execute() returns (ack-mode: record). If the worker
     * crashes mid-run, the message is redelivered to another worker: at-least-once execution.
     */
    @KafkaListener(topics = Topics.RUN_DISPATCH)
    public void onDispatch(String message) throws Exception {
        executor.execute(json.readValue(message, RunDispatch.class));
    }
}
