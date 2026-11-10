package dev.marut.worker.handlers;

import dev.marut.scheduler.contracts.RunDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Demo handlers so the system can be exercised end-to-end without external dependencies. */
@Configuration
public class BuiltInHandlers {

    private static final Logger log = LoggerFactory.getLogger(BuiltInHandlers.class);

    @Bean
    JobHandler logHandler() {
        return handler("log", run -> log.info("Run {} of '{}' payload={}", run.runId(), run.jobName(), run.payload()));
    }

    /** payload: {"millis": 2000} — simulates slow work. */
    @Bean
    JobHandler sleepHandler() {
        return handler("sleep", run -> {
            long ms = ((Number) run.payload().getOrDefault("millis", 1000)).longValue();
            Thread.sleep(ms);
        });
    }

    /** payload: {"failRate": 0.5} — fails randomly (retryably); exercises backoff and retries. */
    @Bean
    JobHandler flakyHandler() {
        return handler("flaky", run -> {
            double rate = ((Number) run.payload().getOrDefault("failRate", 0.5)).doubleValue();
            if (Math.random() < rate) throw new IllegalStateException("Simulated failure");
        });
    }

    /** payload: {"error": "bad input"} — always fails non-retryably → straight to DEAD. */
    @Bean
    JobHandler rejectHandler() {
        return handler("reject", run -> {
            throw new NonRetryableException(String.valueOf(run.payload().getOrDefault("error", "Rejected")));
        });
    }

    @FunctionalInterface
    interface Body { void run(RunDispatch r) throws Exception; }

    private static JobHandler handler(String name, Body body) {
        return new JobHandler() {
            public String name() { return name; }
            public void handle(RunDispatch run) throws Exception { body.run(run); }
        };
    }
}
