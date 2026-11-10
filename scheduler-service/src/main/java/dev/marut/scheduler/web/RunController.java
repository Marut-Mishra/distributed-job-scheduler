package dev.marut.scheduler.web;

import dev.marut.scheduler.run.JobRun;
import dev.marut.scheduler.run.JobRunRepository;
import jakarta.persistence.EntityNotFoundException;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Operator endpoints for the dead-letter workflow: inspect what failed, fix the cause, replay. */
@RestController
@RequestMapping("/api/v1/runs")
public class RunController {

    private final JobRunRepository runs;

    public RunController(JobRunRepository runs) {
        this.runs = runs;
    }

    @GetMapping
    public List<JobResponse.Run> byStatus(@RequestParam JobRun.Status status,
                                          @RequestParam(defaultValue = "50") int limit) {
        return runs.findByStatusOrderByCreatedAtDesc(status, PageRequest.of(0, Math.min(limit, 200)))
                .stream().map(JobResponse.Run::of).toList();
    }

    /** DEAD → RETRY_SCHEDULED(now). The maintenance loop picks it up within a second. */
    @PostMapping("/{id}/replay")
    @Transactional
    public JobResponse.Run replay(@PathVariable UUID id) {
        JobRun run = runs.lockById(id).orElseThrow(() -> new EntityNotFoundException("Run not found: " + id));
        run.replay(Instant.now());
        return JobResponse.Run.of(run);
    }
}
