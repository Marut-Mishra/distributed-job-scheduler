package dev.marut.worker;

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

/**
 * Two kinds of failure, handled in two different places:
 *  - the JOB fails → the handler reports FAILED and the scheduler decides on a retry (business retry)
 *  - the MESSAGE can't be processed at all (bad JSON, can't reach Kafka to report) → retried
 *    in place a couple of times, then parked on job-runs.DLT (transport retry)
 * Without the second, one poison record would block its partition forever.
 */
@Configuration
public class KafkaErrorConfig {

    @Bean
    NewTopic runDispatchDltTopic() {
        return TopicBuilder.name(Topics.RUN_DISPATCH_DLT).partitions(6).replicas(1).build();
    }

    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(2000L, 3));
        handler.addNotRetryableExceptions(JsonProcessingException.class);
        return handler;
    }
}
