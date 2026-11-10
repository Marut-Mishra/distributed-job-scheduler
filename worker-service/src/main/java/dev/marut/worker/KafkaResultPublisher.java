package dev.marut.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.marut.scheduler.contracts.RunResult;
import dev.marut.scheduler.contracts.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaResultPublisher implements ResultPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;

    public KafkaResultPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    @Override
    public void publish(RunResult result) {
        try {
            kafka.send(Topics.RUN_RESULTS, result.runId().toString(), json.writeValueAsString(result))
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publishing result for run " + result.runId(), e);
        } catch (Exception e) {
            // Throwing fails the listener → no offset commit → the dispatch is redelivered.
            throw new IllegalStateException("Could not publish result for run " + result.runId(), e);
        }
    }
}
