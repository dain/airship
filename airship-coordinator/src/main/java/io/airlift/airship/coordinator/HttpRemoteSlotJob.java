package io.airlift.airship.coordinator;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.airship.coordinator.SimpleHttpResponseHandler.SimpleHttpResponseCallback;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.airship.shared.SetThreadName;
import io.airlift.airship.shared.StateMachine;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.TaskStatus;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

public class HttpRemoteSlotJob
        implements RemoteSlotJob
{
    private static final Logger log = Logger.get(HttpRemoteSlotJob.class);

    private final SlotJobId slotJobId;
    private final Executor executor;
    private final JsonCodec<SlotJob> slotJobCodec;
    private final JsonCodec<SlotJobStatus> slotJobStatusCodec;

    private final AsyncHttpClient httpClient;

    private final StateMachine<SlotJobStatus> slotJobStatus;
    private final SlotJob slotJob;

    private final ContinuousSlotJobUpdater continuousTaskInfoFetcher;

    public static HttpRemoteSlotJob createHttpRemoteSlotJob(URI agentUri,
            SlotJob slotJob,
            AsyncHttpClient httpClient,
            Executor executor,
            JsonCodec<SlotJob> slotJobCodec,
            JsonCodec<SlotJobStatus> slotJobStatusCodec)
    {
        HttpRemoteSlotJob httpRemoteSlotJob = new HttpRemoteSlotJob(agentUri, slotJob, httpClient, executor, slotJobCodec, slotJobStatusCodec);
        httpRemoteSlotJob.start();
        return httpRemoteSlotJob;
    }

    private HttpRemoteSlotJob(URI agentUri,
            SlotJob slotJob,
            AsyncHttpClient httpClient,
            Executor executor,
            JsonCodec<SlotJob> slotJobCodec,
            JsonCodec<SlotJobStatus> slotJobStatusCodec)
    {
        this.slotJob = checkNotNull(slotJob, "slotJob is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.slotJobCodec = checkNotNull(slotJobCodec, "slotJobCodec is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.slotJobStatusCodec = checkNotNull(slotJobStatusCodec, "slotJobStatusCodec is null");

        slotJobId = checkNotNull(slotJob, "slotJob is null").getSlotJobId();

        SlotJobStatus initialStatus = new SlotJobStatus(slotJobId,
                uriBuilderFrom(agentUri).replacePath("v1/agent/job").appendPath(slotJobId.toString()).build(),
                SlotJobState.PENDING,
                null,
                null,
                0.0,
                ImmutableList.<TaskStatus>of());

        slotJobStatus = new StateMachine<>("slotJob-" + slotJobId.toString(), executor, initialStatus);

        continuousTaskInfoFetcher = new ContinuousSlotJobUpdater();
    }

    private void start()
    {
        continuousTaskInfoFetcher.start();
    }

    @Override
    public SlotJobStatus getJobStatus()
    {
        return slotJobStatus.get();
    }

    // TODO: depending on the usage, we should only have one of these two wait methods
    public Duration waitForJobStateChange(SlotJobStatus currentState, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName(slotJobStatus.getName())) {
            return slotJobStatus.waitForStateChange(currentState, maxWait);
        }
    }

    public Duration waitForJobStateChange(SlotJobState currentState, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName(slotJobStatus.getName())) {
            while (maxWait.toMillis() > 1) {
                SlotJobStatus jobStatus = getJobStatus();
                if (jobStatus.getState() != currentState) {
                    break;
                }

                maxWait = waitForJobStateChange(jobStatus, maxWait);
            }
            return maxWait;
        }
    }

    public void addStateChangeListener(StateChangeListener<SlotJobStatus> stateChangeListener)
    {
        try (SetThreadName setThreadName = new SetThreadName(slotJobStatus.getName())) {
            slotJobStatus.addStateChangeListener(stateChangeListener);
        }
    }

    public synchronized void cancel()
    {
        try (SetThreadName setThreadName = new SetThreadName("HttpRemoteSlotJob-%s", slotJobId)) {
            // stop updating
            continuousTaskInfoFetcher.stop();

            // mark task as canceled (if not already done)
            SlotJobStatus slotJobStatus = getJobStatus();
            updateSlotJobStatus(new SlotJobStatus(new SlotJobId("id"),
                    slotJobStatus.getSelf(),
                    SlotJobState.CANCELED,
                    slotJobStatus.getSlotStatus(),
                    "failed",
                    100.0,
                    slotJobStatus.getTasks()));

            // fire delete to job and ignore response
            final long start = System.nanoTime();
            final Request request = prepareDelete().setUri(slotJobStatus.getSelf()).build();
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
                    // reschedule
                    if (Duration.nanosSince(start).compareTo(new Duration(2, TimeUnit.MINUTES)) < 0) {
                        Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), this, executor);
                    }
                    else {
                        log.error(t, "Unable to cancel job at %s", request.getUri());
                    }
                }
            }, executor);
        }
    }

    private synchronized void updateSlotJobStatus(final SlotJobStatus newValue)
    {
        // change to new value if job hasn't already been completed
        slotJobStatus.setIf(newValue, new Predicate<SlotJobStatus>()
        {
            public boolean apply(SlotJobStatus oldValue)
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
        SlotJobStatus slotJobStatus = getJobStatus();
        if (!slotJobStatus.getState().isDone()) {
            log.debug(cause, "Slot job failed: %s", slotJobStatus.getSelf());
        }
        updateSlotJobStatus(new SlotJobStatus(new SlotJobId("id"),
                slotJobStatus.getSelf(),
                SlotJobState.FAILED,
                slotJobStatus.getSlotStatus(),
                "failed",
                100.0,
                slotJobStatus.getTasks()));
    }

    /**
     * Continuous update loop for job.  Wait for a short period for job state to change, and
     * if it does not, return the current state of the job.  This will cause stats to be updated at
     * a regular interval, and state changes will be immediately recorded.
     */
    private class ContinuousSlotJobUpdater
            implements SimpleHttpResponseCallback<SlotJobStatus>
    {
        @GuardedBy("this")
        private boolean running;

        @GuardedBy("this")
        private ListenableFuture<JsonResponse<SlotJobStatus>> future;

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
            try (SetThreadName setThreadName = new SetThreadName("ContinuousSlotJobUpdater-%s", slotJobId)) {
                // stopped or done?
                SlotJobStatus slotJobStatus = HttpRemoteSlotJob.this.slotJobStatus.get();
                if (!running || slotJobStatus.getState().isDone()) {
                    return;
                }

                // outstanding request?
                if (future != null && !future.isDone()) {
                    // this should never happen
                    log.error("Can not reschedule update because an update is already running");
                    return;
                }

                Request request;
                if (isFirstRequest) {
                    request = preparePost()
                            .setUri(slotJobStatus.getSelf())
                            .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                            .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(slotJobCodec, slotJob))
                            .build();
                    isFirstRequest = false;
                }
                else {
                    request = prepareGet()
                            .setUri(slotJobStatus.getSelf())
                            .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                            .setHeader(AirshipHeaders.AIRSHIP_CURRENT_STATE, slotJobStatus.getState().toString())
                            .setHeader(AirshipHeaders.AIRSHIP_MAX_WAIT, "200ms")
                            .build();
                }

                future = httpClient.executeAsync(request, createFullJsonResponseHandler(slotJobStatusCodec));
                Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri()), executor);
            }
        }

        @Override
        public void success(SlotJobStatus value)
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousSlotJobUpdater-%s", slotJobId)) {
                synchronized (this) {
                    future = null;
                }

                try {
                    updateSlotJobStatus(value);
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousSlotJobUpdater-%s", slotJobId)) {
                synchronized (this) {
                    future = null;
                }

                failTask(cause);
            }
        }
    }
}
