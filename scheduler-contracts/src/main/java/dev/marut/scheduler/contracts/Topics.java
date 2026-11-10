package dev.marut.scheduler.contracts;

public final class Topics {
    private Topics() { }

    /** Scheduler → workers. Keyed by jobId so runs of one job stay ordered on one partition. */
    public static final String RUN_DISPATCH = "job-runs";

    /** Workers → scheduler. Keyed by runId. */
    public static final String RUN_RESULTS = "job-run-results";

    /** Runs that exhausted retries or failed non-retryably. Business-level dead letters, for alerting/replay. */
    public static final String DEAD_RUNS = "job-runs.dead";

    /** Transport-level dead letters: records that could not even be deserialised (poison messages). */
    public static final String RUN_DISPATCH_DLT = RUN_DISPATCH + ".DLT";
    public static final String RUN_RESULTS_DLT = RUN_RESULTS + ".DLT";
}
