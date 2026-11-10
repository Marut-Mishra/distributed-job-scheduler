package dev.marut.scheduler.claim;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@ConditionalOnProperty(name = "scheduler.claim.enabled", havingValue = "true", matchIfMissing = true)
public class ClaimLoop {

    private static final Logger log = LoggerFactory.getLogger(ClaimLoop.class);
    private static final int MAX_BATCHES_PER_TICK = 20;

    private final DueJobClaimer claimer;
    private final ClaimProperties props;

    public ClaimLoop(DueJobClaimer claimer, ClaimProperties props) {
        this.claimer = claimer;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${scheduler.claim.poll-interval-ms}")
    public void tick() {
        try {
            // A full batch means more may be due: keep draining (bounded) so backlogs clear fast.
            int batches = 0, n;
            do {
                n = claimer.claimBatch(Instant.now());
            } while (n == props.batchSize() && ++batches < MAX_BATCHES_PER_TICK);
        } catch (Exception e) {
            log.error("Claim tick failed; will retry next tick", e);
        }
    }
}
