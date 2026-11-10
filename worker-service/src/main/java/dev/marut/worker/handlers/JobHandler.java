package dev.marut.worker.handlers;

import dev.marut.scheduler.contracts.RunDispatch;

/**
 * One implementation per kind of work. Contract:
 *  - return normally → SUCCEEDED; throw → FAILED
 *  - may be invoked more than once for the same runId (at-least-once delivery),
 *    so side effects must be keyed on {@code run.runId()}.
 */
public interface JobHandler {
    String name();
    void handle(RunDispatch run) throws Exception;
}
