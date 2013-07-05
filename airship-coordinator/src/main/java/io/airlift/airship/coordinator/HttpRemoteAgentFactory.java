package io.airlift.airship.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.airship.shared.AgentLifecycleState;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.InstallationRepresentation;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class HttpRemoteAgentFactory implements RemoteAgentFactory
{
    private final String environment;
    private final AsyncHttpClient httpClient;
    private final JsonCodec<InstallationRepresentation> installationCodec;
    private final JsonCodec<AgentStatus> agentStatusCodec;
    private final JsonCodec<SlotStatus> slotStatusCodec;
    private final JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec;
    private final HttpRemoteSlotJobFactory slotJobFactory;
    private final Executor executor;

    @Inject
    public HttpRemoteAgentFactory(NodeInfo nodeInfo,
            @Global AsyncHttpClient httpClient,
            HttpRemoteSlotJobFactory slotJobFactory,
            JsonCodec<InstallationRepresentation> installationCodec,
            JsonCodec<SlotStatus> slotStatusCodec,
            JsonCodec<AgentStatus> agentStatusCodec,
            JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec)
    {
        environment = nodeInfo.getEnvironment();
        this.httpClient = httpClient;
        this.slotJobFactory = slotJobFactory;
        this.agentStatusCodec = agentStatusCodec;
        this.installationCodec = installationCodec;
        this.slotStatusCodec = slotStatusCodec;
        this.serviceDescriptorsCodec = serviceDescriptorsCodec;
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("agent-%d").build());
    }

    @Override
    public RemoteAgent createRemoteAgent(Instance instance, AgentLifecycleState state)
    {
        AgentStatus agentStatus = new AgentStatus(null,
                state,
                instance.getInstanceId(),
                instance.getInternalUri(),
                instance.getExternalUri(),
                instance.getLocation(),
                instance.getInstanceType(),
                ImmutableList.<SlotStatus>of(),
                ImmutableMap.<String, Integer>of());

        return HttpRemoteAgent.createHttpRemoteAgent(agentStatus,
                environment,
                slotJobFactory,
                httpClient,
                executor,
                installationCodec,
                agentStatusCodec,
                slotStatusCodec,
                serviceDescriptorsCodec);
    }
}
