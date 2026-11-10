package dev.marut.scheduler.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@ConditionalOnProperty(name = "scheduler.maintenance.enabled", havingValue = "true", matchIfMissing = true)
public class MaintenanceLoop {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceLoop.class);
    private final RunMaintenance maintenance;

    public MaintenanceLoop(RunMaintenance maintenance) {
        this.maintenance = maintenance;
    }

    @Scheduled(fixedDelayString = "${scheduler.maintenance.poll-interval-ms:1000}")
    public void tick() {
        try {
            Instant now = Instant.now();
            int reaped = maintenance.reapExpiredLeases(now);
            int retried = maintenance.dispatchDueRetries(now);
            if (reaped + retried > 0) log.info("Maintenance: reaped {} lease(s), dispatched {} retr(y/ies)", reaped, retried);
        } catch (Exception e) {
            log.error("Maintenance tick failed; will retry next tick", e);
        }
    }
}
