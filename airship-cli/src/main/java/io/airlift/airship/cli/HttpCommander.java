package io.airlift.airship.cli;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.airship.coordinator.AgentProvisioningRepresentation;
import io.airlift.airship.coordinator.CoordinatorProvisioningRepresentation;
import io.airlift.airship.coordinator.job.InstallationRequest;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.coordinator.job.LifecycleRequest;
import io.airlift.airship.coordinator.job.SlotLifecycleAction;
import io.airlift.airship.shared.AgentLifecycleState;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.CoordinatorLifecycleState;
import io.airlift.airship.shared.CoordinatorStatusRepresentation;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.ws.rs.core.Response.Status;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class HttpCommander implements Commander
{
    private static final JsonCodec<JobStatus> JOB_INFO_CODEC = JsonCodec.jsonCodec(JobStatus.class);

    private static final JsonCodec<List<SlotStatus>> SLOTS_CODEC = JsonCodec.listJsonCodec(SlotStatus.class);
    private static final JsonCodec<Assignment> ASSIGNMENT_CODEC = JsonCodec.jsonCodec(Assignment.class);

    private static final JsonCodec<List<CoordinatorStatusRepresentation>> COORDINATORS_CODEC = JsonCodec.listJsonCodec(CoordinatorStatusRepresentation.class);
    private static final JsonCodec<CoordinatorProvisioningRepresentation> COORDINATOR_PROVISIONING_CODEC = JsonCodec.jsonCodec(CoordinatorProvisioningRepresentation.class);

    private static final JsonCodec<AgentStatus> AGENT_CODEC = JsonCodec.jsonCodec(AgentStatus.class);
    private static final JsonCodec<List<AgentStatus>> AGENTS_CODEC = JsonCodec.listJsonCodec(AgentStatus.class);
    private static final JsonCodec<AgentProvisioningRepresentation> AGENT_PROVISIONING_CODEC = JsonCodec.jsonCodec(AgentProvisioningRepresentation.class);

    private final JsonCodec<LifecycleRequest> lifecycleRequestCodec = jsonCodec(LifecycleRequest.class);
    private final JsonCodec<InstallationRequest> installationRequestCodec = jsonCodec(InstallationRequest.class);
    private final JsonCodec<JobStatus> jobStatusCodec = jsonCodec(JobStatus.class);
    private final JsonCodec<List<IdAndVersion>> idAndVersionsCodec = listJsonCodec(IdAndVersion.class);

    private final AsyncHttpClient client;
    private final URI coordinatorUri;
    private final boolean useInternalAddress;
    private final Executor executor;

    public HttpCommander(URI coordinatorUri, boolean useInternalAddress, AsyncHttpClient client, Executor executor)
            throws IOException
    {
        Preconditions.checkNotNull(coordinatorUri, "coordinatorUri is null");
        this.coordinatorUri = coordinatorUri;
        this.client = client;
        this.useInternalAddress = useInternalAddress;
        this.executor = executor;
    }

    @Override
    public List<SlotStatus> show(SlotFilter slotFilter)
    {
        URI uri = slotFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot"));
        Request request = Request.Builder.prepareGet()
                .setUri(uri)
                .build();

        JsonResponse<List<SlotStatus>> response = client.execute(request, createFullJsonResponseHandler(SLOTS_CODEC));
        return response.getValue();
    }

    @Override
    public HttpRemoteJob install(List<IdAndVersion> agents, Assignment assignment)
    {
        Request request = Request.Builder.preparePost()
                .setUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(installationRequestCodec, new InstallationRequest(assignment, agents)))
                .build();

        JobStatus jobStatus = client.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));
        return HttpRemoteJob.createHttpRemoteJob(jobStatus, client, executor, jobStatusCodec);
    }

    @Override
    public HttpRemoteJob upgrade(List<IdAndVersion> slots, Assignment assignment, boolean force)
    {
        Request request = Request.Builder.preparePost()
                .setUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot/assignment").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(installationRequestCodec, new InstallationRequest(assignment, slots)))
                .build();

        JobStatus jobStatus = client.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));
        return HttpRemoteJob.createHttpRemoteJob(jobStatus, client, executor, jobStatusCodec);
    }

    @Override
    public HttpRemoteJob doLifecycle(List<IdAndVersion> slots, SlotLifecycleAction action)
    {
        Request request = Request.Builder.preparePost()
                .setUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(action, slots)))
                .build();

        JobStatus jobStatus = client.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));
        return HttpRemoteJob.createHttpRemoteJob(jobStatus, client, executor, jobStatusCodec);
    }

    @Override
    public HttpRemoteJob terminate(List<IdAndVersion> slots)
    {
        Request request = Request.Builder.prepareDelete()
                .setUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(idAndVersionsCodec, slots))
                .build();

        JobStatus job = client.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));
        return HttpRemoteJob.createHttpRemoteJob(job, client, executor, jobStatusCodec);
    }

    @Override
    public HttpRemoteJob resetExpectedState(List<IdAndVersion> slots)
    {
        // todo
        throw new UnsupportedOperationException("not yet implemented");

//        URI uri = slotFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot/expected-state"));
//        Request.Builder requestBuilder = Request.Builder.prepareDelete().setUri(uri);
//        if (expectedVersion != null) {
//            requestBuilder.setHeader(AIRSHIP_SLOTS_VERSION_HEADER, expectedVersion);
//        }
//
//        List<SlotStatusRepresentation> slots = client.execute(requestBuilder.build(), createJsonResponseHandler(SLOTS_CODEC));
//        return slots;
    }

    @Override
    public boolean ssh(SlotFilter slotFilter, String command)
    {
        URI uri = slotFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("/v1/slot"));
        Request request = Request.Builder.prepareGet()
                .setUri(uri)
                .build();

        List<SlotStatus> slots = client.execute(request, createJsonResponseHandler(SLOTS_CODEC));
        if (slots.isEmpty()) {
            return false;
        }
        SlotStatus slot = slots.get(0);
        String host;
        if (useInternalAddress) {
            host = slot.getInternalHost();
        }
        else {
            host = slot.getExternalHost();
        }
        Exec.execRemote(host, slot.getInstallPath(), command);
        return true;
    }

    @Override
    public List<CoordinatorStatusRepresentation> showCoordinators(CoordinatorFilter coordinatorFilter)
    {
        URI uri = coordinatorFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("v1/admin/coordinator"));
        Request request = Request.Builder.prepareGet()
                .setUri(uri)
                .build();

        List<CoordinatorStatusRepresentation> coordinators = client.execute(request, createJsonResponseHandler(COORDINATORS_CODEC));
        return coordinators;
    }

    @Override
    public HttpRemoteJob provisionCoordinators(String coordinatorConfig,
            int coordinatorCount,
            String instanceType,
            String availabilityZone,
            String ami,
            String keyPair,
            String securityGroup,
            boolean waitForStartup)
    {
        URI uri = uriBuilderFrom(coordinatorUri).replacePath("v1/admin/coordinator").build();

        CoordinatorProvisioningRepresentation coordinatorProvisioning = new CoordinatorProvisioningRepresentation(
                coordinatorConfig,
                coordinatorCount,
                instanceType,
                availabilityZone,
                ami,
                keyPair,
                securityGroup);

        Request request = Request.Builder.preparePost()
                .setUri(uri)
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(jsonBodyGenerator(COORDINATOR_PROVISIONING_CODEC, coordinatorProvisioning))
                .build();

        JobStatus jobStatus = client.execute(request, createJsonResponseHandler(JOB_INFO_CODEC));
        return HttpRemoteJob.createHttpRemoteJob(jobStatus, client, executor, jobStatusCodec);
    }

    private List<CoordinatorStatusRepresentation> waitForCoordinatorsToStart(List<String> instanceIds)
    {
        for (int loop = 0; true; loop++) {
            try {
                URI uri = uriBuilderFrom(coordinatorUri).replacePath("v1/admin/coordinator").build();
                Request request = Request.Builder.prepareGet()
                        .setUri(uri)
                        .build();
                List<CoordinatorStatusRepresentation> coordinators = client.execute(request, createJsonResponseHandler(COORDINATORS_CODEC));

                Map<String, CoordinatorStatusRepresentation> runningCoordinators = newHashMap();
                for (CoordinatorStatusRepresentation coordinator : coordinators) {
                    if (coordinator.getState() == CoordinatorLifecycleState.ONLINE) {
                        runningCoordinators.put(coordinator.getInstanceId(), coordinator);
                    }
                }
                if (runningCoordinators.keySet().containsAll(instanceIds)) {
                    WaitUtils.clearWaitMessage();
                    runningCoordinators.keySet().retainAll(instanceIds);
                    return ImmutableList.copyOf(runningCoordinators.values());
                }
            }
            catch (Exception ignored) {
            }

            WaitUtils.wait(loop);
        }
    }

    @Override
    public boolean sshCoordinator(CoordinatorFilter coordinatorFilter, String command)
    {
        URI uri = coordinatorFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("v1/admin/coordinator"));
        Request request = Request.Builder.prepareGet()
                .setUri(uri)
                .build();

        List<CoordinatorStatusRepresentation> coordinators = client.execute(request, createJsonResponseHandler(COORDINATORS_CODEC));
        if (coordinators.isEmpty()) {
            return false;
        }
        Exec.execRemote(coordinators.get(0).getExternalHost(), command);
        return true;
    }

    @Override
    public List<AgentStatus> showAgents(AgentFilter agentFilter)
    {
        URI uri = agentFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("v1/admin/agent"));
        Request request = Request.Builder.prepareGet()
                .setUri(uri)
                .build();

        JsonResponse<List<AgentStatus>> response = client.execute(request, createFullJsonResponseHandler(AGENTS_CODEC));
        if (response.getStatusCode() != 200) {
            throw new RuntimeException(response.getStatusMessage());
        }
        return response.getValue();
    }

    @Override
    public HttpRemoteJob provisionAgents(String agentConfig,
            int agentCount,
            String instanceType,
            String availabilityZone,
            String ami,
            String keyPair,
            String securityGroup,
            boolean waitForStartup)
    {
        URI uri = uriBuilderFrom(coordinatorUri).replacePath("v1/admin/agent").build();

        AgentProvisioningRepresentation agentProvisioning = new AgentProvisioningRepresentation(
                agentConfig,
                agentCount,
                instanceType,
                availabilityZone,
                ami,
                keyPair,
                securityGroup);

        Request request = Request.Builder.preparePost()
                .setUri(uri)
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(jsonBodyGenerator(AGENT_PROVISIONING_CODEC, agentProvisioning))
                .build();

        JobStatus jobStatus = client.execute(request, createJsonResponseHandler(JOB_INFO_CODEC));
        return HttpRemoteJob.createHttpRemoteJob(jobStatus, client, executor, jobStatusCodec);
    }

    private List<AgentStatus> waitForAgentsToStart(List<String> instanceIds)
    {
        for (int loop = 0; true; loop++) {
            try {
                URI uri = uriBuilderFrom(coordinatorUri).replacePath("v1/admin/agent").build();
                Request request = Request.Builder.prepareGet()
                        .setUri(uri)
                        .build();
                List<AgentStatus> agents = client.execute(request, createJsonResponseHandler(AGENTS_CODEC));

                Map<String, AgentStatus> runningAgents = newHashMap();
                for (AgentStatus agent : agents) {
                    if (agent.getState() == AgentLifecycleState.ONLINE) {
                        runningAgents.put(agent.getInstanceId(), agent);
                    }
                }
                if (runningAgents.keySet().containsAll(instanceIds)) {
                    WaitUtils.clearWaitMessage();
                    runningAgents.keySet().retainAll(instanceIds);
                    return ImmutableList.copyOf(runningAgents.values());
                }
            }
            catch (Exception ignored) {
            }

            WaitUtils.wait(loop);
        }
    }

    @Override
    public HttpRemoteJob terminateAgent(String agentId)
    {
        // todo
        throw new UnsupportedOperationException("not yet implemented");
//        URI uri = uriBuilderFrom(coordinatorUri).replacePath("v1/admin/agent").build();
//
//        Request request = Request.Builder.prepareDelete()
//                .setUri(uri)
//                .setBodyGenerator(textBodyGenerator(agentId))
//                .build();
//
//        AgentStatusRepresentation agents = client.execute(request, createJsonResponseHandler(AGENT_CODEC));
//        return agents;
    }

    @Override
    public boolean sshAgent(AgentFilter agentFilter, String command)
    {
        URI uri = agentFilter.toUri(uriBuilderFrom(coordinatorUri).replacePath("v1/admin/agent"));
        Request request = Request.Builder.prepareGet()
                .setUri(uri)
                .build();

        List<AgentStatus> agents = client.execute(request, createJsonResponseHandler(AGENTS_CODEC));
        if (agents.isEmpty()) {
            return false;
        }
        Exec.execRemote(agents.get(0).getExternalHost(), command);
        return true;
    }

    public static class TextBodyGenerator implements BodyGenerator
    {
        public static TextBodyGenerator textBodyGenerator(String instance)
        {
            return new TextBodyGenerator(instance);
        }

        private byte[] text;

        private TextBodyGenerator(String text)
        {
            this.text = text.getBytes(Charsets.UTF_8);
        }

        @Override
        public void write(OutputStream out)
                throws Exception
        {
            out.write(text);
        }
    }
}
