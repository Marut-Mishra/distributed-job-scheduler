package dev.marut.worker;

import dev.marut.scheduler.contracts.RunResult;

public interface ResultPublisher {
    void publish(RunResult result);
}
