package io.airlift.airship.coordinator.job;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.airship.coordinator.Coordinator;
import io.airlift.airship.coordinator.RemoteSlotJob;
import io.airlift.airship.coordinator.job.JobStatus.JobState;
import io.airlift.airship.shared.SetThreadName;
import io.airlift.airship.shared.StateMachine;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.airship.shared.job.SlotJobStatus.isDonePredicate;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class JobExecution
{
    private final JobId jobId;
    private final List<RemoteSlotJob> slotJobs;
    private final StateMachine<JobStatus> status;
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final URI self;

    public static JobExecution createJobExecution(Coordinator coordinator, JobId jobId, List<RemoteSlotJob> slotJobs, Executor executor)
    {
        final JobExecution jobExecution = new JobExecution(coordinator, jobId, slotJobs, executor);
        for (RemoteSlotJob slotJob : slotJobs) {
            slotJob.addStateChangeListener(new StateChangeListener<SlotJobStatus>()
            {
                @Override
                public void stateChanged(SlotJobStatus newValue)
                {
                    jobExecution.updateStatus();
                }
            });
        }

        return jobExecution;
    }

    private JobExecution(Coordinator coordinator, JobId jobId, List<RemoteSlotJob> slotJobs, Executor executor)
    {
        this.jobId = checkNotNull(jobId, "jobId is null");
        this.self = uriBuilderFrom(coordinator.status().getInternalUri()).replacePath("v1/job").appendPath(jobId.toString()).build();
        this.slotJobs = checkNotNull(slotJobs, "tasks is null");
        this.status = new StateMachine<>("job " + jobId, executor, createJobStatus());
    }

    public JobId getJobId()
    {
        return jobId;
    }

    public JobStatus getStatus()
    {
        return status.get();
    }

    public void cancel()
    {
        canceled.set(true);
    }

    public void addStateChangeListener(StateChangeListener<JobStatus> stateChangeListener)
    {
        status.addStateChangeListener(stateChangeListener);
    }

    // TODO: depending on the usage, we should only have one of these two wait methods
    public Duration waitForJobStateChange(JobStatus currentState, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName(status.getName())) {
            return status.waitForStateChange(currentState, maxWait);
        }
    }

    public Duration waitForJobVersionChange(String currentVersion, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName(status.getName())) {
            while (maxWait.toMillis() > 1) {
                JobStatus jobStatus = getStatus();
                if (!jobStatus.getVersion().equals(currentVersion)) {
                    break;
                }

                maxWait = waitForJobStateChange(jobStatus, maxWait);
            }
            return maxWait;
        }
    }

    private synchronized void updateStatus()
    {
        if (status.get().getState().isDone()) {
            return;
        }

        status.set(createJobStatus());
    }

    private JobStatus createJobStatus()
    {
        ImmutableList<SlotJobStatus> slotJobStatuses = ImmutableList.copyOf(transform(slotJobs, slotJobStatusGetter()));

        JobState jobState;
        if (Iterables.all(slotJobStatuses, isDonePredicate())) {
            jobState = JobState.DONE;
        }
        else {
            jobState = JobState.RUNNING;
        }

        // Be careful in this code!  There is a race condition between
        // this job and the slot.  Always get job state before slot
        // state because the job can finish between getting the slot state
        // and the job state.

        return new JobStatus(jobId, self, jobState, slotJobStatuses);
    }

    private Function<RemoteSlotJob, SlotJobStatus> slotJobStatusGetter()
    {
        return new Function<RemoteSlotJob, SlotJobStatus>()
        {
            @Override
            public SlotJobStatus apply(RemoteSlotJob remoteSlotJob)
            {
                return remoteSlotJob.getJobStatus();
            }
        };
    }
}
