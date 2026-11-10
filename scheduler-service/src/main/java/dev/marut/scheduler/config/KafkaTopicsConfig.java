package dev.marut.scheduler.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.marut.scheduler.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaTopicsConfig {

    // Partition count caps consumer parallelism per group: 6 partitions → up to 6 active consumers.
    // DLTs get the same count because the recoverer publishes to the same partition number.
    @Bean NewTopic runDispatchTopic()    { return topic(Topics.RUN_DISPATCH); }
    @Bean NewTopic runResultsTopic()     { return topic(Topics.RUN_RESULTS); }
    @Bean NewTopic deadRunsTopic()       { return topic(Topics.DEAD_RUNS); }
    @Bean NewTopic runResultsDltTopic()  { return topic(Topics.RUN_RESULTS_DLT); }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(6).replicas(1).build();
    }

    /**
     * Poison-message protection for the results consumer. A record that keeps failing is retried
     * twice, then parked on job-run-results.DLT so it cannot block its partition forever.
     * Malformed JSON is never retried: it will not get better.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
        handler.addNotRetryableExceptions(JsonProcessingException.class);
        return handler;
    }
}
