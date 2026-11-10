package dev.marut.scheduler.web;

import dev.marut.scheduler.job.JobService;
import dev.marut.scheduler.run.JobRunRepository;
import jakarta.validation.Valid;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService service;
    private final JobRunRepository runs;

    public JobController(JobService service, JobRunRepository runs) {
        this.service = service;
        this.runs = runs;
    }

    /** 201 on first create, 200 when an idempotent retry returns the existing job. */
    @PostMapping
    public ResponseEntity<JobResponse> create(@Valid @RequestBody CreateJobRequest req) {
        var result = service.create(req);
        var body = JobResponse.of(result.job());
        return result.created()
                ? ResponseEntity.created(URI.create("/api/v1/jobs/" + body.id())).body(body)
                : ResponseEntity.status(HttpStatus.OK).body(body);
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return JobResponse.of(service.get(id));
    }

    @GetMapping("/{id}/runs")
    public List<JobResponse.Run> runs(@PathVariable UUID id, @RequestParam(defaultValue = "20") int limit) {
        service.get(id); // 404 if the job doesn't exist
        return runs.findByJobIdOrderByCreatedAtDesc(id, PageRequest.of(0, Math.min(limit, 100)))
                .stream().map(JobResponse.Run::of).toList();
    }

    @PostMapping("/{id}/pause")
    public JobResponse pause(@PathVariable UUID id) { return JobResponse.of(service.pause(id)); }

    @PostMapping("/{id}/resume")
    public JobResponse resume(@PathVariable UUID id) { return JobResponse.of(service.resume(id)); }
}
