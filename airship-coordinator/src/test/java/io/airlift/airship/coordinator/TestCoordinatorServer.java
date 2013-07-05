/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.airship.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.airlift.airship.coordinator.job.InstallationRequest;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.coordinator.job.LifecycleRequest;
import io.airlift.airship.coordinator.job.SlotLifecycleAction;
import io.airlift.airship.shared.AgentLifecycleState;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.CoordinatorLifecycleState;
import io.airlift.airship.shared.CoordinatorStatus;
import io.airlift.airship.shared.CoordinatorStatusRepresentation;
import io.airlift.airship.shared.ExpectedSlotStatus;
import io.airlift.airship.shared.HttpUriBuilder;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.Repository;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.core.Response.Status;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.airship.coordinator.CoordinatorSlotResource.MIN_PREFIX_SIZE;
import static io.airlift.airship.coordinator.TestingMavenRepository.MOCK_REPO;
import static io.airlift.airship.shared.AgentLifecycleState.ONLINE;
import static io.airlift.airship.shared.AssignmentHelper.APPLE_ASSIGNMENT;
import static io.airlift.airship.shared.AssignmentHelper.BANANA_ASSIGNMENT;
import static io.airlift.airship.shared.ExtraAssertions.assertEqualsNoOrder;
import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.airship.shared.IdAndVersion.forIds;
import static io.airlift.airship.shared.SlotLifecycleState.RUNNING;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotStatus.shortenSlotStatus;
import static io.airlift.airship.shared.Strings.shortestUniquePrefix;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestCoordinatorServer
{
    private HttpClient httpClient;
    private TestingHttpServer server;

    private Coordinator coordinator;
    private MockProvisioner provisioner;
    private InMemoryStateManager stateManager;

    private final JsonCodec<List<CoordinatorStatusRepresentation>> coordinatorStatusesCodec = listJsonCodec(CoordinatorStatusRepresentation.class);
    private final JsonCodec<List<AgentStatus>> agentStatusesCodec = listJsonCodec(AgentStatus.class);
    private final JsonCodec<List<SlotStatus>> slotStatusesCodec = listJsonCodec(SlotStatus.class);
    private final JsonCodec<CoordinatorProvisioningRequest> coordinatorProvisioningCodec = jsonCodec(CoordinatorProvisioningRequest.class);
    private final JsonCodec<AgentProvisioningRequest> agentProvisioningCodec = jsonCodec(AgentProvisioningRequest.class);

    private final JsonCodec<LifecycleRequest> lifecycleRequestCodec = jsonCodec(LifecycleRequest.class);
    private final JsonCodec<InstallationRequest> installationRequestCodec = jsonCodec(InstallationRequest.class);
    private final JsonCodec<JobStatus> jobStatusCodec = jsonCodec(JobStatus.class);
    private final JsonCodec<List<IdAndVersion>> idAndVersionsCodec = listJsonCodec(IdAndVersion.class);

    private String agentId;
    private UUID apple1SlotId;
    private UUID apple2SlotId;
    private UUID bananaSlotId;

    @BeforeClass
    public void startServer()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("airship.version", "123")
                .put("node.id", "this-coordinator-instance-id")
                .put("node.location", "/test/location")
                .put("coordinator.binary-repo", "http://localhost:9999/")
                .put("coordinator.default-group-id", "prod")
                .put("coordinator.agent.default-config", "@agent.config")
                .put("coordinator.aws.access-key", "my-access-key")
                .put("coordinator.aws.secret-key", "my-secret-key")
                .put("coordinator.aws.agent.ami", "ami-0123abcd")
                .put("coordinator.aws.agent.keypair", "keypair")
                .put("coordinator.aws.agent.security-group", "default")
                .put("coordinator.aws.agent.default-instance-type", "t1.micro")
                .build();

        Injector injector = Guice.createInjector(new TestingHttpServerModule(),
                new TestingNodeModule(),
                new JsonModule(),
                new JaxrsModule(),
                new EventModule(),
                Modules.override(new StaticProvisionerModule()).with(new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(StateManager.class).to(InMemoryStateManager.class).in(SINGLETON);
                        binder.bind(MockProvisioner.class).in(SINGLETON);
                        binder.bind(Provisioner.class).to(Key.get(MockProvisioner.class)).in(SINGLETON);
                    }
                }),
                Modules.override(new CoordinatorMainModule()).with(new Module()
                {
                    public void configure(Binder binder)
                    {
                        binder.bind(Repository.class).toInstance(MOCK_REPO);
                        binder.bind(ServiceInventory.class).to(MockServiceInventory.class).in(Scopes.SINGLETON);
                    }

                    @Provides
                    @Singleton
                    public RemoteCoordinatorFactory getRemoteCoordinatorFactory(MockProvisioner provisioner)
                    {
                        return provisioner.getCoordinatorFactory();
                    }

                    @Provides
                    @Singleton
                    public RemoteAgentFactory getRemoteAgentFactory(MockProvisioner provisioner)
                    {
                        return provisioner.getAgentFactory();
                    }
                }),
                new ConfigurationModule(new ConfigurationFactory(properties)));

        server = injector.getInstance(TestingHttpServer.class);
        coordinator = injector.getInstance(Coordinator.class);
        stateManager = (InMemoryStateManager) injector.getInstance(StateManager.class);
        provisioner = (MockProvisioner) injector.getInstance(Provisioner.class);

        server.start();
        httpClient = new ApacheHttpClient();
    }

    @BeforeMethod
    public void resetState()
    {
        provisioner.clearCoordinators();
        coordinator.updateAllCoordinatorsAndWait();
        assertEquals(coordinator.getCoordinators().size(), 1);

        provisioner.clearAgents();
        coordinator.updateAllAgents();
        assertTrue(coordinator.getAgents().isEmpty());

        stateManager.clearAll();
        assertTrue(coordinator.getAllSlotsStatus().isEmpty());
    }

    private void initializeOneAgent()
    {
        apple1SlotId = UUID.randomUUID();
        SlotStatus appleSlotStatus1 = new SlotStatus(
                apple1SlotId,
                apple1SlotId.toString(),
                URI.create("fake://appleServer1/v1/agent/slot/apple1"),
                URI.create("fake://appleServer1/v1/agent/slot/apple1"),
                "instance",
                "/location",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/apple1",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                APPLE_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(apple1SlotId, STOPPED, APPLE_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(appleSlotStatus1));

        apple2SlotId = UUID.randomUUID();
        SlotStatus appleSlotStatus2 = new SlotStatus(
                apple2SlotId,
                apple2SlotId.toString(),
                URI.create("fake://appleServer2/v1/agent/slot/apple1"),
                URI.create("fake://appleServer2/v1/agent/slot/apple1"),
                "instance",
                "/location",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/apple2",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                APPLE_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(apple2SlotId, STOPPED, APPLE_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(appleSlotStatus2));

        bananaSlotId = UUID.randomUUID();
        SlotStatus bananaSlotStatus = new SlotStatus(
                bananaSlotId,
                bananaSlotId.toString(),
                URI.create("fake://bananaServer/v1/agent/slot/banana"),
                URI.create("fake://bananaServer/v1/agent/slot/banana"),
                "instance",
                "/location",
                "/location",
                STOPPED,
                BANANA_ASSIGNMENT,
                "/banana",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                BANANA_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(bananaSlotId, STOPPED, BANANA_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(bananaSlotStatus));

        agentId = UUID.randomUUID().toString();
        AgentStatus agentStatus = new AgentStatus(agentId,
                ONLINE,
                "instance-id",
                URI.create("fake://foo/"),
                URI.create("fake://foo/"),
                "/unknown/location",
                "instance.type",
                ImmutableList.of(appleSlotStatus1, appleSlotStatus2, bananaSlotStatus),
                ImmutableMap.of("cpu", 8, "memory", 1024));

        provisioner.addAgents(agentStatus);
        coordinator.updateAllAgents();

//        stateManager.clearAll();
    }

    @AfterClass
    public void stopServer()
            throws Exception
    {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testGetAllCoordinatorsSingle()
            throws Exception
    {
        // add a coordinator directly to the provisioner
        String coordinatorId = UUID.randomUUID().toString();
        String instanceId = "instance-id";
        URI internalUri = URI.create("fake://coordinator/" + instanceId + "/internal");
        URI externalUri = URI.create("fake://coordinator/" + instanceId + "/external");
        String location = "/unknown/location";
        String instanceType = "instance.type";

        CoordinatorStatus status = new CoordinatorStatus(coordinatorId,
                CoordinatorLifecycleState.ONLINE,
                instanceId,
                internalUri,
                externalUri,
                location,
                instanceType);
        provisioner.addCoordinators(status);
        coordinator.updateAllCoordinatorsAndWait();

        // verify coordinator appears
        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/coordinator").build())
                .build();

        List<CoordinatorStatusRepresentation> coordinators = httpClient.execute(request, createJsonResponseHandler(coordinatorStatusesCodec, Status.OK.getStatusCode()));
        CoordinatorStatusRepresentation actual = getNonMainCoordinator(coordinators);

        assertEquals(actual.getCoordinatorId(), coordinatorId);
        assertEquals(actual.getState(), CoordinatorLifecycleState.ONLINE);
        assertEquals(actual.getInstanceId(), instanceId);
        assertEquals(actual.getLocation(), location);
        assertEquals(actual.getInstanceType(), instanceType);
        assertEquals(actual.getSelf(), internalUri);
        assertEquals(actual.getExternalUri(), externalUri);
    }

    @Test
    public void testCoordinatorProvision()
            throws Exception
    {
        // provision the coordinator and verify
        String instanceType = "instance-type";
        CoordinatorProvisioningRequest coordinatorProvisioningRequest = new CoordinatorProvisioningRequest("coordinator:config:1", 1, instanceType, null, null, null, null);
        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/coordinator").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(coordinatorProvisioningCodec, coordinatorProvisioningRequest))
                .build();
        List<CoordinatorStatusRepresentation> coordinators = httpClient.execute(request, createJsonResponseHandler(coordinatorStatusesCodec, Status.OK.getStatusCode()));

        assertEquals(coordinators.size(), 1);
        String instanceId = coordinators.get(0).getInstanceId();
        assertNotNull(instanceId);
        String location = coordinators.get(0).getLocation();
        assertNotNull(location);
        assertEquals(coordinators.get(0).getInstanceType(), instanceType);
        assertNull(coordinators.get(0).getCoordinatorId());
        assertNull(coordinators.get(0).getSelf());
        assertNull(coordinators.get(0).getExternalUri());
        assertEquals(coordinators.get(0).getState(), CoordinatorLifecycleState.PROVISIONING);

        // start the coordinator and verify
        CoordinatorStatus expectedCoordinatorStatus = provisioner.startCoordinator(instanceId);
        coordinator.updateAllCoordinatorsAndWait();
        assertEquals(coordinator.getCoordinators().size(), 2);
        assertEquals(coordinator.getCoordinator(instanceId).getInstanceId(), instanceId);
        assertEquals(coordinator.getCoordinator(instanceId).getInstanceType(), instanceType);
        assertEquals(coordinator.getCoordinator(instanceId).getLocation(), location);
        assertEquals(coordinator.getCoordinator(instanceId).getCoordinatorId(), expectedCoordinatorStatus.getCoordinatorId());
        assertEquals(coordinator.getCoordinator(instanceId).getInternalUri(), expectedCoordinatorStatus.getInternalUri());
        assertEquals(coordinator.getCoordinator(instanceId).getExternalUri(), expectedCoordinatorStatus.getExternalUri());
        assertEquals(coordinator.getCoordinator(instanceId).getState(), CoordinatorLifecycleState.ONLINE);


        request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/coordinator").build())
                .build();

        coordinators = httpClient.execute(request, createJsonResponseHandler(coordinatorStatusesCodec, Status.OK.getStatusCode()));
        CoordinatorStatusRepresentation actual = getNonMainCoordinator(coordinators);

        assertEquals(actual.getInstanceId(), instanceId);
        assertEquals(actual.getInstanceType(), instanceType);
        assertEquals(actual.getLocation(), location);
        assertEquals(actual.getCoordinatorId(), expectedCoordinatorStatus.getCoordinatorId());
        assertEquals(actual.getSelf(), expectedCoordinatorStatus.getInternalUri());
        assertEquals(actual.getExternalUri(), expectedCoordinatorStatus.getExternalUri());
        assertEquals(actual.getState(), CoordinatorLifecycleState.ONLINE);
    }

    private CoordinatorStatusRepresentation getNonMainCoordinator(List<CoordinatorStatusRepresentation> coordinators)
    {
        assertEquals(coordinators.size(), 2);
        CoordinatorStatusRepresentation actual;
        if (coordinators.get(0).getInstanceId().equals(coordinator.status().getInstanceId())) {
            actual = coordinators.get(1);
        }
        else {
            actual = coordinators.get(0);
            assertEquals(coordinators.get(1).getInstanceId(), coordinator.status().getInstanceId());
        }
        return actual;
    }

    @Test
    public void testGetAllAgentsEmpty()
    {
        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/agent").build())
                .build();

        List<AgentStatus> actual = httpClient.execute(request, createJsonResponseHandler(agentStatusesCodec, Status.OK.getStatusCode()));
        assertEquals(actual.size(), 0);
    }

    @Test
    public void testGetAllAgentsSingle()
            throws Exception
    {
        String agentId = UUID.randomUUID().toString();
        URI internalUri = URI.create("fake://agent/" + agentId + "/internal");
        URI externalUri = URI.create("fake://agent/" + agentId + "/external");
        String instanceId = "instance-id";
        String location = "/unknown/location";
        String instanceType = "instance.type";
        Map<String, Integer> resources = ImmutableMap.of("cpu", 8, "memory", 1024);

        AgentStatus status = new AgentStatus(agentId,
                AgentLifecycleState.ONLINE,
                instanceId,
                internalUri,
                externalUri,
                location,
                instanceType,
                ImmutableList.<SlotStatus>of(),
                resources);

        // add the agent
        provisioner.addAgents(status);
        coordinator.updateAllAgents();

        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/agent").build())
                .build();

        List<AgentStatus> agents = httpClient.execute(request, createJsonResponseHandler(agentStatusesCodec, Status.OK.getStatusCode()));
        assertEquals(agents.size(), 1);

        AgentStatus actual = agents.get(0);
        assertEquals(actual.getAgentId(), agentId);
        assertEquals(actual.getState(), AgentLifecycleState.ONLINE);
        assertEquals(actual.getInstanceId(), instanceId);
        assertEquals(actual.getLocation(), location);
        assertEquals(actual.getInstanceType(), instanceType);
        assertEquals(actual.getInternalUri(), internalUri);
        assertEquals(actual.getExternalUri(), externalUri);
        assertEquals(actual.getResources(), resources);
    }

    @Test
    public void testAgentProvision()
            throws Exception
    {
        // provision the agent and verify
        String instanceType = "instance-type";
        AgentProvisioningRequest agentProvisioningRequest = new AgentProvisioningRequest("agent:config:1", 1, instanceType, null, null, null, null);
        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/agent").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(agentProvisioningCodec, agentProvisioningRequest))
                .build();
        List<AgentStatus> agents = httpClient.execute(request, createJsonResponseHandler(agentStatusesCodec, Status.OK.getStatusCode()));

        assertEquals(agents.size(), 1);
        String instanceId = agents.get(0).getInstanceId();
        assertNotNull(instanceId);
        String location = agents.get(0).getLocation();
        assertNotNull(location);
        assertEquals(agents.get(0).getInstanceType(), instanceType);
        assertNull(agents.get(0).getAgentId());
        assertNull(agents.get(0).getInternalUri());
        assertNull(agents.get(0).getExternalUri());
        assertEquals(agents.get(0).getState(), AgentLifecycleState.PROVISIONING);

        // start the agent and verify
        AgentStatus expectedAgentStatus = provisioner.startAgent(instanceId);
        coordinator.updateAllAgents();
        assertEquals(coordinator.getAgents().size(), 1);
        assertEquals(coordinator.getAgent(instanceId).getInstanceId(), instanceId);
        assertEquals(coordinator.getAgent(instanceId).getInstanceType(), instanceType);
        assertEquals(coordinator.getAgent(instanceId).getLocation(), location);
        assertEquals(coordinator.getAgent(instanceId).getAgentId(), expectedAgentStatus.getAgentId());
        assertEquals(coordinator.getAgent(instanceId).getInternalUri(), expectedAgentStatus.getInternalUri());
        assertEquals(coordinator.getAgent(instanceId).getExternalUri(), expectedAgentStatus.getExternalUri());
        assertEquals(coordinator.getAgent(instanceId).getState(), AgentLifecycleState.ONLINE);

        request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/agent").build())
                .build();

        agents = httpClient.execute(request, createJsonResponseHandler(agentStatusesCodec, Status.OK.getStatusCode()));
        assertEquals(agents.size(), 1);

        AgentStatus actual = agents.get(0);
        assertEquals(actual.getInstanceId(), instanceId);
        assertEquals(actual.getInstanceType(), instanceType);
        assertEquals(actual.getLocation(), location);
        assertEquals(actual.getAgentId(), expectedAgentStatus.getAgentId());
        assertEquals(actual.getInternalUri(), expectedAgentStatus.getInternalUri());
        assertEquals(actual.getExternalUri(), expectedAgentStatus.getExternalUri());
        assertEquals(actual.getState(), AgentLifecycleState.ONLINE);
    }

    @Test
    public void testGetAllSlots()
            throws Exception
    {
        initializeOneAgent();

        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot").addParameter("name", "*").build())
                .build();
        List<SlotStatus> actual = httpClient.execute(request, createJsonResponseHandler(slotStatusesCodec, Status.OK.getStatusCode()));

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);

        int prefixSize = shortestUniquePrefix(asList(
                agentStatus.getSlot(apple1SlotId).getId().toString(),
                agentStatus.getSlot(apple2SlotId).getId().toString(),
                agentStatus.getSlot(bananaSlotId).getId().toString()),
                MIN_PREFIX_SIZE);

        assertEqualsNoOrder(actual, ImmutableList.of(
                shortenSlotStatus(agentStatus.getSlot(apple1SlotId), prefixSize, 0, MOCK_REPO),
                shortenSlotStatus(agentStatus.getSlot(apple2SlotId), prefixSize, 0, MOCK_REPO),
                shortenSlotStatus(agentStatus.getSlot(bananaSlotId), prefixSize, 0, MOCK_REPO)));
    }

    @Test
    public void testUpgrade()
            throws Exception
    {
        initializeOneAgent();

        Assignment upgradeVersions = new Assignment("2.0", "2.0");
        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/assignment").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(installationRequestCodec, new InstallationRequest(upgradeVersions, IdAndVersion.forIds(apple1SlotId, apple2SlotId))))
                .build();

        httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);
        SlotStatus apple1Status = agentStatus.getSlot(apple1SlotId);
        SlotStatus apple2Status = agentStatus.getSlot(apple2SlotId);
        SlotStatus bananaStatus = agentStatus.getSlot(bananaSlotId);

        assertEquals(apple1Status.getState(), STOPPED);
        assertEquals(apple2Status.getState(), STOPPED);
        assertEquals(bananaStatus.getState(), STOPPED);

        assertEquals(apple1Status.getAssignment(), upgradeVersions.upgradeAssignment(MOCK_REPO, APPLE_ASSIGNMENT));
        assertEquals(apple2Status.getAssignment(), upgradeVersions.upgradeAssignment(MOCK_REPO, APPLE_ASSIGNMENT));
        assertEquals(bananaStatus.getAssignment(), BANANA_ASSIGNMENT);
    }

    @Test
    public void testTerminate()
            throws Exception
    {
        initializeOneAgent();

        Request request = Request.Builder.prepareDelete()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(idAndVersionsCodec, IdAndVersion.forIds(apple1SlotId, apple2SlotId)))
                .build();

        httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        SlotStatus apple1StatusActual = coordinator.getAgentByAgentId(agentId).getSlot(apple1SlotId);
        SlotStatus apple2StatusActual = coordinator.getAgentByAgentId(agentId).getSlot(apple2SlotId);
        SlotStatus bananaStatus = coordinator.getAgentByAgentId(agentId).getSlot(bananaSlotId);

        assertNull(apple1StatusActual);
        assertNull(apple2StatusActual);
        assertEquals(bananaStatus.getState(), STOPPED);
    }

    @Test
    public void testStart()
            throws Exception
    {
        initializeOneAgent();

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.START, forIds(apple1SlotId, apple2SlotId))))
                .build();

        httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);
        SlotStatus apple1Status = agentStatus.getSlot(apple1SlotId);
        SlotStatus apple2Status = agentStatus.getSlot(apple2SlotId);
        SlotStatus bananaStatus = agentStatus.getSlot(bananaSlotId);

        assertEquals(apple1Status.getState(), RUNNING);
        assertEquals(apple2Status.getState(), RUNNING);
        assertEquals(bananaStatus.getState(), STOPPED);
    }

    @Test
    public void testRestart()
            throws Exception
    {
        initializeOneAgent();

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.RESTART, forIds(apple1SlotId, apple2SlotId))))
                .build();

        httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);
        SlotStatus apple1Status = agentStatus.getSlot(apple1SlotId);
        SlotStatus apple2Status = agentStatus.getSlot(apple2SlotId);
        SlotStatus bananaStatus = agentStatus.getSlot(bananaSlotId);

        assertEquals(apple1Status.getState(), RUNNING);
        assertEquals(apple2Status.getState(), RUNNING);
        assertEquals(bananaStatus.getState(), STOPPED);
    }

    @Test
    public void testStop()
            throws Exception
    {
        initializeOneAgent();

        coordinator.doLifecycle(forIds(apple1SlotId, apple2SlotId, bananaSlotId), SlotLifecycleAction.START);

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.STOP, forIds(apple1SlotId, apple2SlotId))))
                .build();

        httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);
        SlotStatus apple1Status = agentStatus.getSlot(apple1SlotId);
        SlotStatus apple2Status = agentStatus.getSlot(apple2SlotId);
        SlotStatus bananaStatus = agentStatus.getSlot(bananaSlotId);

        assertEquals(apple1Status.getState(), STOPPED);
        assertEquals(apple2Status.getState(), STOPPED);
        assertEquals(bananaStatus.getState(), RUNNING);
    }

    @Test
    public void testKill()
            throws Exception
    {
        initializeOneAgent();

        coordinator.doLifecycle(forIds(apple1SlotId, apple2SlotId, bananaSlotId), SlotLifecycleAction.START);

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.KILL, forIds(apple1SlotId, apple2SlotId))))
                .build();

        httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);
        SlotStatus apple1Status = agentStatus.getSlot(apple1SlotId);
        SlotStatus apple2Status = agentStatus.getSlot(apple2SlotId);
        SlotStatus bananaStatus = agentStatus.getSlot(bananaSlotId);

        assertEquals(apple1Status.getState(), STOPPED);
        assertEquals(apple2Status.getState(), STOPPED);
        assertEquals(bananaStatus.getState(), RUNNING);
    }

    private HttpUriBuilder coordinatorUriBuilder()
    {
        return uriBuilderFrom(server.getBaseUrl());
    }
}
