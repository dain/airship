package io.airlift.airship.cli;

import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.units.Duration;

public interface RemoteJob
{
    void addStateChangeListener(StateChangeListener<JobStatus> stateChangeListener);

    void cancel();

    JobStatus getJobStatus();

    Duration waitForJobVersionChange(String currentVersion, Duration maxWait)
                    throws InterruptedException;
}
