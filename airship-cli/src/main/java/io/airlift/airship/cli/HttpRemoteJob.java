package io.airlift.airship.cli;

import com.google.common.base.Predicate;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.airship.coordinator.SimpleHttpResponseHandler;
import io.airlift.airship.coordinator.SimpleHttpResponseHandler.SimpleHttpResponseCallback;
import io.airlift.airship.coordinator.job.JobId;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.coordinator.job.JobStatus.JobState;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.airship.shared.SetThreadName;
import io.airlift.airship.shared.StateMachine;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

public class HttpRemoteJob
        implements RemoteJob
{
    private static final Logger log = Logger.get(HttpRemoteJob.class);

    private final JobId jobId;
    private final StateMachine<JobStatus> jobStatus;
    private final Executor executor;
    private final JsonCodec<JobStatus> jobStatusCodec;

    private final AsyncHttpClient httpClient;

    private final ContinuousJobUpdater continuousTaskInfoFetcher;

    public static HttpRemoteJob createHttpRemoteJob(
            JobStatus jobStatus,
            AsyncHttpClient httpClient,
            Executor executor,
            JsonCodec<JobStatus> jobStatusCodec)
    {
        HttpRemoteJob httpRemoteJob = new HttpRemoteJob(jobStatus, httpClient, executor, jobStatusCodec);
        httpRemoteJob.start();
        return httpRemoteJob;
    }

    private HttpRemoteJob(
            JobStatus jobStatus,
            AsyncHttpClient httpClient,
            Executor executor,
            JsonCodec<JobStatus> jobStatusCodec)
    {
        this.jobId = checkNotNull(jobStatus, "jobStatus is null").getJobId();
        this.jobStatus = new StateMachine<>("job-" + jobId.toString(), executor, jobStatus);
        this.executor = checkNotNull(executor, "executor is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.jobStatusCodec = checkNotNull(jobStatusCodec, "jobStatusCodec is null");


        continuousTaskInfoFetcher = new ContinuousJobUpdater();
    }

    private void start()
    {
        continuousTaskInfoFetcher.start();
    }

    @Override
    public JobStatus getJobStatus()
    {
        return jobStatus.get();
    }

    // TODO: depending on the usage, we should only have one of these two wait methods
    public Duration waitForJobStateChange(JobStatus currentState, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName(jobStatus.getName())) {
            return jobStatus.waitForStateChange(currentState, maxWait);
        }
    }

    @Override
    public Duration waitForJobVersionChange(String currentVersion, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName(jobStatus.getName())) {
            while (maxWait.toMillis() > 1) {
                JobStatus jobStatus = getJobStatus();
                if (!jobStatus.getVersion().equals(currentVersion)) {
                    break;
                }

                maxWait = waitForJobStateChange(jobStatus, maxWait);
            }
            return maxWait;
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<JobStatus> stateChangeListener)
    {
        try (SetThreadName setThreadName = new SetThreadName(jobStatus.getName())) {
            jobStatus.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public synchronized void cancel()
    {
        try (SetThreadName setThreadName = new SetThreadName("HttpRemoteJob-%s", jobId)) {
            // stop updating
            continuousTaskInfoFetcher.stop();

            // mark task as canceled (if not already done)
            JobStatus jobStatus = getJobStatus();
            updateJobStatus(new JobStatus(new JobId("id"),
                    jobStatus.getSelf(),
                    JobState.CANCELED,
                    jobStatus.getSlotJobStatuses()));

            // fire delete to job and ignore response
            final Request request = prepareDelete().setUri(jobStatus.getSelf()).build();
            Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), new FutureCallback<StatusResponse>()
            {
                @Override
                public void onSuccess(StatusResponse result)
                {
                    // assume any response is good enough
                }

                @Override
                public void onFailure(Throwable t)
                {
                    log.error(t, "Unable to cancel job at %s", request.getUri());
                }
            }, executor);
        }
    }

    private synchronized void updateJobStatus(final JobStatus newValue)
    {
        // change to new value if job hasn't already been completed
        jobStatus.setIf(newValue, new Predicate<JobStatus>()
        {
            public boolean apply(JobStatus oldValue)
            {
                if (oldValue.getState().isDone()) {
                    // never update if the task has reached a terminal state
                    return false;
                }
                return true;
            }
        });
    }

    /**
     * Move the task directly to the failed state
     */
    private void failTask(Throwable cause)
    {
        // todo store cause somewhere
        JobStatus jobStatus = getJobStatus();
        if (!jobStatus.getState().isDone()) {
            log.debug(cause, "Job failed: %s", jobStatus.getSelf());
        }
        updateJobStatus(new JobStatus(new JobId("id"),
                jobStatus.getSelf(),
                JobState.FAILED,
                jobStatus.getSlotJobStatuses()));
    }

    /**
     * Continuous update loop for job.  Wait for a short period for job state to change, and
     * if it does not, return the current state of the job.  This will cause stats to be updated at
     * a regular interval, and state changes will be immediately recorded.
     */
    private class ContinuousJobUpdater
            implements SimpleHttpResponseCallback<JobStatus>
    {
        @GuardedBy("this")
        private boolean running;

        @GuardedBy("this")
        private ListenableFuture<JsonResponse<JobStatus>> future;

        @GuardedBy("this")
        private boolean isFirstRequest = true;

        public synchronized void start()
        {
            if (running) {
                // already running
                return;
            }
            running = true;
            scheduleNextRequest();
        }

        public synchronized void stop()
        {
            running = false;
            if (future != null) {
                future.cancel(true);
                future = null;
            }
        }

        private synchronized void scheduleNextRequest()
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousJobUpdater-%s", jobId)) {
                // stopped or done?
                JobStatus jobStatus = HttpRemoteJob.this.jobStatus.get();
                if (!running || jobStatus.getState().isDone()) {
                    return;
                }

                // outstanding request?
                if (future != null && !future.isDone()) {
                    // this should never happen
                    log.error("Can not reschedule update because an update is already running");
                    return;
                }

                Request request = prepareGet()
                        .setUri(jobStatus.getSelf())
                        .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                        .setHeader(AirshipHeaders.AIRSHIP_CURRENT_STATE, jobStatus.getVersion())
                        .setHeader(AirshipHeaders.AIRSHIP_MAX_WAIT, "200ms")
                        .build();

                future = httpClient.executeAsync(request, createFullJsonResponseHandler(jobStatusCodec));
                Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri()), executor);
            }
        }

        @Override
        public void success(JobStatus value)
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousJobUpdater-%s", jobId)) {
                synchronized (this) {
                    future = null;
                }

                try {
                    updateJobStatus(value);
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousJobUpdater-%s", jobId)) {
                synchronized (this) {
                    future = null;
                }

                failTask(cause);
            }
        }
    }
}
