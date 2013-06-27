package io.airlift.airship.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.airship.coordinator.SimpleHttpResponseHandler.SimpleHttpResponseCallback;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.AgentStatusRepresentation;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.InstallationRepresentation;
import io.airlift.airship.shared.SetThreadName;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.airship.shared.StateMachine;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.Response.Status;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.airship.shared.AgentLifecycleState.OFFLINE;
import static io.airlift.airship.shared.AgentLifecycleState.ONLINE;
import static io.airlift.airship.shared.AgentLifecycleState.PROVISIONING;
import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class HttpRemoteAgent
        implements RemoteAgent
{
    private static final Logger log = Logger.get(HttpRemoteAgent.class);

    private final JsonCodec<InstallationRepresentation> installationCodec;
    private final JsonCodec<AgentStatusRepresentation> agentStatusCodec;
    private final JsonCodec<SlotStatusRepresentation> slotStatusCodec;
    private final JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec;
    private final HttpRemoteSlotJobFactory slotJobFactory;

    private final String agentId;
    private final StateMachine<AgentStatus> agentStatus;
    private final Executor executor;
    private final String environment;
    private final AsyncHttpClient httpClient;

    private final AtomicBoolean serviceInventoryUp = new AtomicBoolean(true);

    private final ContinuousAgentUpdater continuousAgentUpdater;

    public static HttpRemoteAgent createHttpRemoteAgent(AgentStatus agentStatus,
            String environment,
            HttpRemoteSlotJobFactory slotJobFactory,
            AsyncHttpClient httpClient,
            Executor executor,
            JsonCodec<InstallationRepresentation> installationCodec,
            JsonCodec<AgentStatusRepresentation> agentStatusCodec,
            JsonCodec<SlotStatusRepresentation> slotStatusCodec,
            JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec)
    {
        HttpRemoteAgent httpRemoteAgent = new HttpRemoteAgent(agentStatus,
                environment,
                slotJobFactory,
                httpClient,
                executor,
                installationCodec,
                agentStatusCodec,
                slotStatusCodec,
                serviceDescriptorsCodec);
        httpRemoteAgent.start();
        return httpRemoteAgent;
    }

    private HttpRemoteAgent(AgentStatus agentStatus,
            String environment,
            HttpRemoteSlotJobFactory slotJobFactory,
            AsyncHttpClient httpClient,
            Executor executor,
            JsonCodec<InstallationRepresentation> installationCodec,
            JsonCodec<AgentStatusRepresentation> agentStatusCodec,
            JsonCodec<SlotStatusRepresentation> slotStatusCodec,
            JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec)
    {
        this.agentId = agentStatus.getAgentId();
        this.agentStatus = new StateMachine<>("agent " + agentId, executor, checkNotNull(agentStatus, "agentStatus is null"));
        this.environment = checkNotNull(environment, "environment is null");
        this.slotJobFactory = checkNotNull(slotJobFactory, "slotJobFactory is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.installationCodec = checkNotNull(installationCodec, "installationCodec is null");
        this.agentStatusCodec = checkNotNull(agentStatusCodec, "agentStatusCodec is null");
        this.slotStatusCodec = checkNotNull(slotStatusCodec, "slotStatusCodec is null");
        this.serviceDescriptorsCodec = checkNotNull(serviceDescriptorsCodec, "serviceDescriptorsCodec is null");
        this.continuousAgentUpdater = new ContinuousAgentUpdater();
    }

    public void start()
    {
        continuousAgentUpdater.start();
    }

    @Override
    public void stop()
    {
        continuousAgentUpdater.stop();
    }

    @Override
    public synchronized AgentStatus status()
    {
        return agentStatus.get();
    }

    public Duration waitForStateChange(AgentStatus currentState, Duration maxWait)
            throws InterruptedException
    {
        return agentStatus.waitForStateChange(currentState, maxWait);
    }

    public Duration waitForVersionChange(String version, Duration maxWait)
            throws InterruptedException
    {
        while (maxWait.toMillis() > 1) {
            AgentStatus agentStatus = status();
            if (Objects.equal(agentStatus.getVersion(), version)) {
                break;
            }

            maxWait = waitForStateChange(agentStatus, maxWait);
        }
        return maxWait;
    }



    @Override
    public void setInternalUri(URI internalUri)
    {
        while (true) {
            AgentStatus currentStatus = agentStatus.get();
            AgentStatus newStatus = currentStatus.changeInternalUri(internalUri);
            if (agentStatus.compareAndSet(currentStatus, newStatus)) {
                continuousAgentUpdater.start();
                return;
            }
        }
    }

    @Override
    public synchronized List<? extends RemoteSlot> getSlots()
    {
        return ImmutableList.copyOf(Iterables.transform(status().getSlotStatuses(), new Function<SlotStatus, HttpRemoteSlot>()
        {
            @Override
            public HttpRemoteSlot apply(SlotStatus slotStatus)
            {
                return new HttpRemoteSlot(slotStatus, httpClient, HttpRemoteAgent.this);
            }
        }));
    }

    @Override
    public HttpRemoteSlotJob createSlotJob(SlotJob slotJob)
    {
        return slotJobFactory.createHttpRemoteSlotJob(this.status().getInternalUri(), slotJob);
    }

    @Override
    public void setServiceInventory(List<ServiceDescriptor> serviceInventory)
    {
        AgentStatus agentStatus = status();
        if (agentStatus.getState() == ONLINE) {
            checkNotNull(serviceInventory, "serviceInventory is null");
            final URI internalUri = agentStatus.getInternalUri();
            Request request = Request.Builder.preparePut()
                    .setUri(uriBuilderFrom(internalUri).replacePath("/v1/serviceInventory").build())
                    .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                    .setBodyGenerator(jsonBodyGenerator(serviceDescriptorsCodec, new ServiceDescriptorsRepresentation(environment, serviceInventory)))
                    .build();

            Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), new FutureCallback<StatusResponseHandler.StatusResponse>()
            {
                @Override
                public void onSuccess(StatusResponse result)
                {
                    if (serviceInventoryUp.compareAndSet(false, true)) {
                        log.info("Service inventory put succeeded for agent at %s", internalUri);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    if (serviceInventoryUp.compareAndSet(true, false) && !log.isDebugEnabled()) {
                        log.error("Unable to post service inventory to agent at %s: %s", internalUri, t.getMessage());
                    }
                    log.debug(t, "Unable to post service inventory to agent at %s: %s", internalUri, t.getMessage());
                }
            });
        }
    }

    public synchronized void setStatus(AgentStatus agentStatus)
    {
        checkNotNull(agentStatus, "agentStatus is null");
        this.agentStatus.set(agentStatus);
    }

    public synchronized void setSlotStatus(SlotStatus slotStatus)
    {
        while (true) {
            AgentStatus currentStatus = agentStatus.get();
            AgentStatus newStatus = currentStatus.changeSlotStatus(slotStatus);
            if (agentStatus.compareAndSet(currentStatus, newStatus)) {
                return;
            }
        }
    }

    @Override
    public SlotStatus install(Installation installation)
    {
        checkNotNull(installation, "installation is null");
        AgentStatus agentStatus = status();
        URI internalUri = agentStatus.getInternalUri();
        Preconditions.checkState(internalUri != null, "agent is down");
        try {
            Request request = Request.Builder.preparePost()
                    .setUri(uriBuilderFrom(internalUri).replacePath("/v1/agent/slot/").build())
                    .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                    .setBodyGenerator(jsonBodyGenerator(installationCodec, InstallationRepresentation.from(installation)))
                    .build();
            SlotStatusRepresentation slotStatusRepresentation = httpClient.execute(request, createJsonResponseHandler(slotStatusCodec, Status.CREATED.getStatusCode()));

            SlotStatus slotStatus = slotStatusRepresentation.toSlotStatus(agentStatus.getInstanceId());
            setStatus(agentStatus.changeSlotStatus(slotStatus));

            return slotStatus;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Continuous update loop for job.  Wait for a short period for job state to change, and
     * if it does not, return the current state of the agent.  This will cause stats to be updated at
     * a regular interval, and state changes will be immediately recorded.
     */
    private class ContinuousAgentUpdater
            implements SimpleHttpResponseCallback<AgentStatusRepresentation>
    {
        @GuardedBy("this")
        private ListenableFuture<JsonResponse<AgentStatusRepresentation>> future;

        private final AtomicLong failureCount = new AtomicLong();

        public synchronized void start()
        {
            scheduleNextRequest();
        }

        public synchronized void stop()
        {
            ///
            //
            //  TODO
            //
            //

        }

        private synchronized void scheduleNextRequest()
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousAgentUpdater-%s", agentId)) {
                AgentStatus agentStatus = HttpRemoteAgent.this.agentStatus.get();
                if (agentStatus.getInternalUri() == null) {
                    return;
                }

                // outstanding request?
                if (future != null && !future.isDone()) {
                    log.debug("Can not reschedule update because an update is already running");
                    return;
                }

                Request request = Request.Builder.prepareGet()
                        .setUri(uriBuilderFrom(agentStatus.getInternalUri()).replacePath("/v1/agent/").build())
                        .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                        .setHeader(AirshipHeaders.AIRSHIP_CURRENT_STATE, agentStatus.getVersion())
                        .setHeader(AirshipHeaders.AIRSHIP_MAX_WAIT, "200ms")
                        .build();

                future = httpClient.executeAsync(request, createFullJsonResponseHandler(agentStatusCodec));
                Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri()), executor);
            }
        }

        @Override
        public void success(AgentStatusRepresentation value)
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousAgentUpdater-%s", agentId)) {
                synchronized (this) {
                    future = null;
                }

                failureCount.set(0);

                try {
                    AgentStatus currentStatus = agentStatus.get();
                    AgentStatus newStatus = value.toAgentStatus(currentStatus.getInstanceId(), currentStatus.getInstanceType());
                    // todo deal with out of order responses
                    agentStatus.set(newStatus);
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName setThreadName = new SetThreadName("ContinuousAgentUpdater-%s", agentId)) {
                synchronized (this) {
                    future = null;
                }

                // error talking to agent -- mark agent offline
                AgentStatus agentStatus = HttpRemoteAgent.this.agentStatus.get();
                if (agentStatus.getState() != PROVISIONING && failureCount.incrementAndGet() > 5) {
                    setStatus(agentStatus.changeState(OFFLINE).changeAllSlotsState(SlotLifecycleState.UNKNOWN));
                }

                try {
                    log.debug(cause, "Error updating agent status at %s", agentStatus.getAgentId());
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }
    }
}

