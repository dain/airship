package io.airlift.airship.cli;

import io.airlift.airship.coordinator.Coordinator;
import io.airlift.airship.coordinator.job.JobId;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.units.Duration;

public class LocalRemoteJob
        implements RemoteJob
{
    private final JobId jobId;
    private final Coordinator coordinator;

    public LocalRemoteJob(JobId jobId, Coordinator coordinator)
    {
        this.jobId = jobId;
        this.coordinator = coordinator;
    }

    @Override

    public JobStatus getJobStatus()
    {
        return coordinator.getJobStatus(jobId);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<JobStatus> stateChangeListener)
    {
        coordinator.addStateChangeListener(jobId, stateChangeListener);
    }

    @Override
    public Duration waitForJobVersionChange(String currentVersion, Duration maxWait)
            throws InterruptedException
    {
        return coordinator.waitForJobVersionChange(jobId, currentVersion, maxWait);
    }

    @Override
    public void cancel()
    {
        coordinator.cancelJob(jobId);
    }
}
