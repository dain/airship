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
package io.airlift.airship.integration;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.airlift.airship.agent.Agent;
import io.airlift.airship.agent.Slot;
import io.airlift.airship.coordinator.AgentProvisioningRequest;
import io.airlift.airship.coordinator.Coordinator;
import io.airlift.airship.coordinator.CoordinatorMainModule;
import io.airlift.airship.coordinator.CoordinatorProvisioningRequest;
import io.airlift.airship.coordinator.HttpRemoteAgent;
import io.airlift.airship.coordinator.HttpRemoteSlotJob;
import io.airlift.airship.coordinator.HttpRemoteSlotJobFactory;
import io.airlift.airship.coordinator.InMemoryStateManager;
import io.airlift.airship.coordinator.Instance;
import io.airlift.airship.coordinator.Provisioner;
import io.airlift.airship.coordinator.StateManager;
import io.airlift.airship.coordinator.StaticProvisionerModule;
import io.airlift.airship.coordinator.TestingMavenRepository;
import io.airlift.airship.coordinator.job.InstallationRequest;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.coordinator.job.LifecycleRequest;
import io.airlift.airship.coordinator.job.SlotLifecycleAction;
import io.airlift.airship.integration.MockLocalProvisioner.AgentServer;
import io.airlift.airship.integration.MockLocalProvisioner.CoordinatorServer;
import io.airlift.airship.shared.AgentLifecycleState;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.CoordinatorLifecycleState;
import io.airlift.airship.shared.CoordinatorStatusRepresentation;
import io.airlift.airship.shared.HttpUriBuilder;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.Repository;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.SlotStatus.SlotStatusFactory;
import io.airlift.airship.shared.job.InstallTask;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.StartTask;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import io.airlift.units.Duration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response.Status;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.airship.shared.AssignmentHelper.APPLE_ASSIGNMENT;
import static io.airlift.airship.shared.AssignmentHelper.BANANA_ASSIGNMENT;
import static io.airlift.airship.shared.ExtraAssertions.assertEqualsNoOrder;
import static io.airlift.airship.shared.FileUtils.createTempDir;
import static io.airlift.airship.shared.FileUtils.deleteRecursively;
import static io.airlift.airship.shared.FileUtils.newFile;
import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.airship.shared.IdAndVersion.forIds;
import static io.airlift.airship.shared.InstallationHelper.APPLE_INSTALLATION;
import static io.airlift.airship.shared.SlotLifecycleState.RUNNING;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static io.airlift.airship.shared.job.SlotJobStatus.succeededPredicate;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Assertions.assertNotEquals;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestServerIntegration
{
    private AsyncHttpClient httpClient;

    private TestingHttpServer coordinatorServer;
    private Coordinator coordinator;
    private InMemoryStateManager stateManager;
    private MockLocalProvisioner provisioner;

    private Slot appleSlot1;
    private Slot appleSlot2;
    private Slot bananaSlot;

    private final JsonCodec<List<CoordinatorStatusRepresentation>> coordinatorStatusesCodec = listJsonCodec(CoordinatorStatusRepresentation.class);
    private final JsonCodec<List<AgentStatus>> agentStatusesCodec = listJsonCodec(AgentStatus.class);
    private final JsonCodec<List<SlotStatus>> slotStatusesCodec = listJsonCodec(SlotStatus.class);
    private final JsonCodec<CoordinatorProvisioningRequest> coordinatorProvisioningCodec = jsonCodec(CoordinatorProvisioningRequest.class);
    private final JsonCodec<AgentProvisioningRequest> agentProvisioningCodec = jsonCodec(AgentProvisioningRequest.class);

    private final JsonCodec<LifecycleRequest> lifecycleRequestCodec = jsonCodec(LifecycleRequest.class);
    private final JsonCodec<InstallationRequest> installationRequestCodec = jsonCodec(InstallationRequest.class);
    private final JsonCodec<JobStatus> jobStatusCodec = jsonCodec(JobStatus.class);
    private final JsonCodec<List<IdAndVersion>> idAndVersionsCodec = listJsonCodec(IdAndVersion.class);

    private File binaryRepoDir;
    private File localBinaryRepoDir;
    private File expectedStateDir;
    private File serviceInventoryCacheDir;
    private Repository repository;

    private String agentInstanceId;
    private SlotStatusFactory slotStatusFactory;
    private HttpRemoteSlotJobFactory httpRemoteSlotJobFactory;

    @BeforeClass
    public void startServer()
            throws Exception
    {
        try {
            binaryRepoDir = TestingMavenRepository.createBinaryRepoDir();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        localBinaryRepoDir = createTempDir("localBinaryRepoDir");
        expectedStateDir = createTempDir("expected-state");
        serviceInventoryCacheDir = createTempDir("service-inventory-cache");

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .put("node.environment", "prod")
                .put("airship.version", "123")
                .put("coordinator.binary-repo", binaryRepoDir.toURI().toString())
                .put("coordinator.default-group-id", "prod")
                .put("coordinator.binary-repo.local", localBinaryRepoDir.toString())
                .put("coordinator.status.expiration", "100d")
                .put("coordinator.agent.default-config", "@agent.config")
                .put("coordinator.aws.access-key", "my-access-key")
                .put("coordinator.aws.secret-key", "my-secret-key")
                .put("coordinator.aws.agent.ami", "ami-0123abcd")
                .put("coordinator.aws.agent.keypair", "keypair")
                .put("coordinator.aws.agent.security-group", "default")
                .put("coordinator.aws.agent.default-instance-type", "t1.micro")
                .put("coordinator.expected-state.dir", expectedStateDir.getAbsolutePath())
                .put("coordinator.service-inventory.cache-dir", serviceInventoryCacheDir.getAbsolutePath())
                .build();

        Injector coordinatorInjector = Guice.createInjector(new TestingHttpServerModule(),
                new NodeModule(),
                new JsonModule(),
                new JaxrsModule(),
                new EventModule(),
                new CoordinatorMainModule(),
                Modules.override(new StaticProvisionerModule()).with(new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(StateManager.class).to(InMemoryStateManager.class).in(SINGLETON);
                        binder.bind(Provisioner.class).to(MockLocalProvisioner.class).in(SINGLETON);
                    }
                }),
                new ConfigurationModule(new ConfigurationFactory(coordinatorProperties)));

        coordinatorServer = coordinatorInjector.getInstance(TestingHttpServer.class);
        coordinator = coordinatorInjector.getInstance(Coordinator.class);
        repository = coordinatorInjector.getInstance(Repository.class);
        stateManager = (InMemoryStateManager) coordinatorInjector.getInstance(StateManager.class);
        provisioner = (MockLocalProvisioner) coordinatorInjector.getInstance(Provisioner.class);

        coordinatorServer.start();

        httpClient = new StandaloneNettyAsyncHttpClient("test");
        httpRemoteSlotJobFactory = new HttpRemoteSlotJobFactory(httpClient, jsonCodec(SlotJob.class), jsonCodec(SlotJobStatus.class));
    }

    @BeforeMethod
    public void resetState()
            throws Exception
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

    @AfterClass
    public void stopServer()
            throws Exception
    {
        provisioner.clearAgents();
        provisioner.clearCoordinators();

        if (coordinatorServer != null) {
            coordinatorServer.stop();
        }

        if (binaryRepoDir != null) {
            deleteRecursively(binaryRepoDir);
        }
        if (expectedStateDir != null) {
            deleteRecursively(expectedStateDir);
        }
        if (serviceInventoryCacheDir != null) {
            deleteRecursively(serviceInventoryCacheDir);
        }
        if (localBinaryRepoDir != null) {
            deleteRecursively(localBinaryRepoDir);
        }

        if (httpClient != null) {
            httpClient.close();
        }
    }

    private Agent initializeOneAgent()
            throws Exception
    {
        List<Instance> instances = provisioner.provisionAgents("agent:config:1", 1, "instance-type", null, null, null, null);
        assertEquals(instances.size(), 1);
        AgentServer agentServer = provisioner.getAgent(instances.get(0).getInstanceId());
        assertNotNull(agentServer);
        agentServer.start();
        agentInstanceId = agentServer.getInstanceId();

        Agent agent = agentServer.getAgent();
        appleSlot1 = agent.getSlot(agent.install(new Installation(
                APPLE_ASSIGNMENT,
                repository.binaryToHttpUri(APPLE_ASSIGNMENT.getBinary()),
                repository.configToHttpUri(APPLE_ASSIGNMENT.getConfig()),
                ImmutableMap.of("memory", 512))).getId());
        appleSlot2 = agent.getSlot(agent.install(new Installation(
                APPLE_ASSIGNMENT,
                repository.binaryToHttpUri(APPLE_ASSIGNMENT.getBinary()),
                repository.configToHttpUri(APPLE_ASSIGNMENT.getConfig()),
                ImmutableMap.of("memory", 512))).getId());
        bananaSlot = agent.getSlot(agent.install(new Installation(
                BANANA_ASSIGNMENT,
                repository.binaryToHttpUri(BANANA_ASSIGNMENT.getBinary()),
                repository.configToHttpUri(BANANA_ASSIGNMENT.getConfig()),
                ImmutableMap.of("memory", 512))).getId());

        coordinator.updateAllAgents();
        waitForAgentToBeOnline(agentInstanceId);

        assertEquals(coordinator.getAgents().size(), 1);
        assertNotNull(coordinator.getAgent(agentServer.getInstanceId()));
        assertEquals(coordinator.getAgent(agentServer.getInstanceId()).getState(), AgentLifecycleState.ONLINE);
        assertNotNull(coordinator.getAgent(agentServer.getInstanceId()).getInternalUri());
        assertNotNull(coordinator.getAgent(agentServer.getInstanceId()).getExternalUri());

        slotStatusFactory = new SlotStatusFactory(ImmutableList.of(appleSlot1.status(), appleSlot2.status(), bananaSlot.status()), repository);
        return agent;
    }

    @Test
    public void testGetAllCoordinatorsSingle()
            throws Exception
    {
        // directly add a new coordinator and start it
        List<Instance> instances = provisioner.provisionCoordinators("coordinator:config:1", 1, "instance-type", null, null, null, null);
        assertEquals(instances.size(), 1);
        CoordinatorServer coordinatorServer = provisioner.getCoordinator(instances.get(0).getInstanceId());
        coordinatorServer.start();
        coordinator.updateAllCoordinatorsAndWait();

        // verify coordinator appears
        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/coordinator").build())
                .build();

        List<CoordinatorStatusRepresentation> coordinators = httpClient.execute(request, createJsonResponseHandler(coordinatorStatusesCodec, Status.OK.getStatusCode()));
        CoordinatorStatusRepresentation actual = getNonMainCoordinator(coordinators);

        assertEquals(actual.getCoordinatorId(), coordinatorServer.getCoordinator().status().getCoordinatorId());
        assertEquals(actual.getState(), CoordinatorLifecycleState.ONLINE);
        assertEquals(actual.getInstanceId(), coordinatorServer.getInstance().getInstanceId());
        assertEquals(actual.getLocation(), coordinatorServer.getInstance().getLocation());
        assertEquals(actual.getInstanceType(), coordinatorServer.getInstance().getInstanceType());
        assertEquals(actual.getSelf(), coordinatorServer.getInstance().getInternalUri());
        assertEquals(actual.getExternalUri(), coordinatorServer.getInstance().getExternalUri());
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
        CoordinatorServer coordinatorServer = provisioner.getCoordinator(instanceId);
        coordinatorServer.start();
        coordinator.updateAllCoordinatorsAndWait();
        assertEquals(coordinator.getCoordinators().size(), 2);
        assertEquals(coordinator.getCoordinator(instanceId).getInstanceId(), instanceId);
        assertEquals(coordinator.getCoordinator(instanceId).getInstanceType(), instanceType);
        assertEquals(coordinator.getCoordinator(instanceId).getLocation(), location);
        assertEquals(coordinator.getCoordinator(instanceId).getCoordinatorId(), coordinatorServer.getCoordinator().status().getCoordinatorId());
        assertEquals(coordinator.getCoordinator(instanceId).getInternalUri(), coordinatorServer.getInstance().getInternalUri());
        assertEquals(coordinator.getCoordinator(instanceId).getExternalUri(), coordinatorServer.getInstance().getExternalUri());
        assertEquals(coordinator.getCoordinator(instanceId).getState(), CoordinatorLifecycleState.ONLINE);


        request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/coordinator").build())
                .build();

        coordinators = httpClient.execute(request, createJsonResponseHandler(coordinatorStatusesCodec, Status.OK.getStatusCode()));
        CoordinatorStatusRepresentation actual = getNonMainCoordinator(coordinators);

        assertEquals(actual.getInstanceId(), instanceId);
        assertEquals(actual.getInstanceType(), instanceType);
        assertEquals(actual.getLocation(), location);
        assertEquals(actual.getCoordinatorId(), coordinatorServer.getCoordinator().status().getCoordinatorId());
        assertEquals(actual.getSelf(), coordinatorServer.getInstance().getInternalUri());
        assertEquals(actual.getExternalUri(), coordinatorServer.getInstance().getExternalUri());
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
        // directly add a new agent and start it
        List<Instance> instances = provisioner.provisionAgents("agent:config:1", 1, "instance-type", null, null, null, null);
        assertEquals(instances.size(), 1);
        AgentServer agentServer = provisioner.getAgent(instances.get(0).getInstanceId());
        agentServer.start();
        coordinator.updateAllAgents();
        waitForAgentToBeOnline(instances.get(0).getInstanceId());

        // get list of all agents
        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/admin/agent").build())
                .build();

        // verify agents list contains only the agent provisioned above
        List<AgentStatus> agents = httpClient.execute(request, createJsonResponseHandler(agentStatusesCodec, Status.OK.getStatusCode()));
        assertEquals(agents.size(), 1);
        AgentStatus actual = agents.get(0);
        assertEquals(actual.getAgentId(), agentServer.getAgent().getAgentId());
        assertEquals(actual.getState(), AgentLifecycleState.ONLINE);
        assertEquals(actual.getInstanceId(), agentServer.getInstance().getInstanceId());
        assertEquals(actual.getLocation(), agentServer.getInstance().getLocation());
        assertEquals(actual.getInstanceType(), agentServer.getInstance().getInstanceType());
        assertNotNull(actual.getInternalUri());
        assertEquals(actual.getInternalUri(), agentServer.getInstance().getInternalUri());
        assertNotNull(actual.getExternalUri());
        assertEquals(actual.getExternalUri(), agentServer.getInstance().getExternalUri());
        assertEquals(actual.getResources(), agentServer.getAgent().getAgentStatus().getResources());
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
        AgentServer agentServer = provisioner.getAgent(agents.get(0).getInstanceId());
        agentServer.start();
        coordinator.updateAllAgents();
        waitForAgentToBeOnline(instanceId);
        assertEquals(coordinator.getAgents().size(), 1);
        assertEquals(coordinator.getAgent(instanceId).getInstanceId(), instanceId);
        assertEquals(coordinator.getAgent(instanceId).getInstanceType(), instanceType);
        assertEquals(coordinator.getAgent(instanceId).getLocation(), location);
        assertEquals(coordinator.getAgent(instanceId).getAgentId(), agentServer.getAgent().getAgentId());
        assertNotNull(coordinator.getAgent(instanceId).getInternalUri());
        assertEquals(coordinator.getAgent(instanceId).getInternalUri(), agentServer.getInstance().getInternalUri());
        assertNotNull(coordinator.getAgent(instanceId).getExternalUri());
        assertEquals(coordinator.getAgent(instanceId).getExternalUri(), agentServer.getInstance().getExternalUri());
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
        assertEquals(actual.getAgentId(), agentServer.getAgent().getAgentId());
        assertNotNull(actual.getInternalUri());
        assertEquals(actual.getInternalUri(), agentServer.getInstance().getInternalUri());
        assertNotNull(actual.getExternalUri());
        assertEquals(actual.getExternalUri(), agentServer.getInstance().getExternalUri());
        assertEquals(actual.getState(), AgentLifecycleState.ONLINE);
    }

    @Test
    public void testStart()
            throws Exception
    {
        Agent agent = initializeOneAgent();
        waitForAgentStatusPropagation(agent);

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.START, forIds(appleSlot1.getId(), appleSlot2.getId()))))
                .build();

        JobStatus job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        job = waitForCompletion(job);
        assertSuccess(job);

        assertEquals(appleSlot1.status().getState(), RUNNING);
        assertEquals(appleSlot2.status().getState(), RUNNING);
        assertEquals(bananaSlot.status().getState(), STOPPED);
    }

    @Test
    public void testUpgrade()
            throws Exception
    {
        Agent agent = initializeOneAgent();
        waitForAgentStatusPropagation(agent);

        Assignment upgradeVersions = new Assignment("2.0", "@2.0");

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/assignment").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(installationRequestCodec, new InstallationRequest(upgradeVersions, forIds(appleSlot1.getId(), appleSlot2.getId()))))
                .build();

        JobStatus job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        job = waitForCompletion(job);
        assertSuccess(job);

        assertEquals(appleSlot1.status().getState(), STOPPED);
        assertEquals(appleSlot2.status().getState(), STOPPED);
        assertEquals(bananaSlot.status().getState(), STOPPED);

        assertEquals(appleSlot1.status().getAssignment(), upgradeVersions.upgradeAssignment(repository, APPLE_ASSIGNMENT));
        assertEquals(appleSlot2.status().getAssignment(), upgradeVersions.upgradeAssignment(repository, APPLE_ASSIGNMENT));
    }

    @Test
    public void testTerminate()
            throws Exception
    {
        Agent agent = initializeOneAgent();
        waitForAgentStatusPropagation(agent);

        Request request = Request.Builder.prepareDelete()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(idAndVersionsCodec, forIds(appleSlot1.getId(), appleSlot2.getId())))
                .build();

        JobStatus job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        job = waitForCompletion(job);
        assertSuccess(job);

        assertEquals(appleSlot1.status().getState(), TERMINATED);
        assertEquals(appleSlot2.status().getState(), TERMINATED);
        assertEquals(bananaSlot.status().getState(), STOPPED);
    }

    @Test
    public void testRestart()
            throws Exception
    {
        Agent agent = initializeOneAgent();

        appleSlot1.start();
        waitForAgentStatusPropagation(agent);
        assertEquals(appleSlot1.status().getState(), RUNNING);

        File pidFile = newFile(appleSlot1.status().getInstallPath(), "..", "installation", "launcher.pid").getCanonicalFile();
        String pidBeforeRestart = Files.readFirstLine(pidFile, Charsets.UTF_8);


        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.RESTART, forIds(appleSlot1.getId(), appleSlot2.getId()))))
                .build();

        JobStatus job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        job = waitForCompletion(job);
        assertSuccess(job);

        assertEquals(appleSlot1.status().getState(), RUNNING);
        assertEquals(appleSlot2.status().getState(), RUNNING);
        assertEquals(bananaSlot.status().getState(), STOPPED);

        String pidAfterRestart = Files.readFirstLine(pidFile, Charsets.UTF_8);
        assertNotEquals(pidAfterRestart, pidBeforeRestart);
    }

    @Test
    public void testStop()
            throws Exception
    {
        Agent agent = initializeOneAgent();

        appleSlot1.start();
        appleSlot2.start();
        bananaSlot.start();
        waitForAgentStatusPropagation(agent);

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.STOP, forIds(appleSlot1.getId(), appleSlot2.getId()))))
                .build();

        JobStatus job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        job = waitForCompletion(job);
        assertSuccess(job);

        assertEquals(appleSlot1.status().getState(), STOPPED);
        assertEquals(appleSlot2.status().getState(), STOPPED);
        assertEquals(bananaSlot.status().getState(), RUNNING);
    }


    @Test
    public void testKill()
            throws Exception
    {
        Agent agent = initializeOneAgent();

        appleSlot1.start();
        appleSlot2.start();
        bananaSlot.start();
        waitForAgentStatusPropagation(agent);

        Request request = Request.Builder.preparePost()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot/lifecycle").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(lifecycleRequestCodec, new LifecycleRequest(SlotLifecycleAction.KILL, forIds(appleSlot1.getId(), appleSlot2.getId()))))
                .build();

        JobStatus job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));

        job = waitForCompletion(job);
        assertSuccess(job);

        assertEquals(appleSlot1.status().getState(), STOPPED);
        assertEquals(appleSlot2.status().getState(), STOPPED);
        assertEquals(bananaSlot.status().getState(), RUNNING);
    }

    @Test
    public void testShow()
            throws Exception
    {
        Agent agent = initializeOneAgent();
        waitForAgentStatusPropagation(agent);

        Request request = Request.Builder.prepareGet()
                .setUri(coordinatorUriBuilder().appendPath("/v1/slot").addParameter("!binary", "*:apple:*").build())
                .build();
        List<SlotStatus> actual = httpClient.execute(request, createJsonResponseHandler(slotStatusesCodec, Status.OK.getStatusCode()));

        List<SlotStatus> expected = ImmutableList.of(
                slotStatusFactory.create(bananaSlot.status().changeInstanceId(agentInstanceId)));
        assertEqualsNoOrder(actual, expected);
    }

    @Test
    public void testSlotInstallationAndStartJob()
            throws Exception
    {
        Agent agent = initializeOneAgent();
        waitForAgentStatusPropagation(agent);

        Installation foo = new Installation(
                APPLE_ASSIGNMENT,
                repository.binaryToHttpUri(APPLE_ASSIGNMENT.getBinary()),
                repository.configToHttpUri(APPLE_ASSIGNMENT.getConfig()),
                ImmutableMap.of("test", 999));

        SlotJob slotJob = new SlotJob(new SlotJobId("job"), null, ImmutableList.of(new InstallTask(foo), new StartTask()));
        HttpRemoteSlotJob httpRemoteSlotJob = httpRemoteSlotJobFactory.createHttpRemoteSlotJob(agent.getAgentStatus().getInternalUri(), slotJob);
        SlotJobStatus slotJobStatus = httpRemoteSlotJob.getJobStatus();
        assertNotNull(slotJobStatus, "slotJobStatus is null");

        // wait for job to start
        if (slotJobStatus.getState() == SlotJobState.PENDING) {
            httpRemoteSlotJob.waitForJobStateChange(SlotJobState.PENDING, new Duration(1, TimeUnit.MINUTES));
            slotJobStatus = httpRemoteSlotJob.getJobStatus();
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        Assert.assertNotEquals(slotJobStatus.getState(), SlotJobState.PENDING);

        // wait for job to finish
        if (slotJobStatus.getState() == SlotJobState.RUNNING) {
            httpRemoteSlotJob.waitForJobStateChange(SlotJobState.RUNNING, new Duration(1, TimeUnit.MINUTES));
            slotJobStatus = httpRemoteSlotJob.getJobStatus();
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        Assert.assertNotEquals(slotJobStatus.getState(), SlotJobState.RUNNING);

        // verify done
        assertEquals(slotJobStatus.getState(), SlotJobState.DONE);
        SlotStatus slotStatus = slotJobStatus.getSlotStatus();
        assertNotNull(slotStatus, "slotStatus is null");
        assertEquals(slotStatus.getAssignment().getBinary(), APPLE_INSTALLATION.getAssignment().getBinary());
        assertEquals(slotStatus.getAssignment().getConfig(), APPLE_INSTALLATION.getAssignment().getConfig());
        try {
            assertEquals(slotStatus.getState(), SlotLifecycleState.RUNNING);
        }
        catch (Throwable t) {
            assertEquals(slotStatus.getState(), SlotLifecycleState.RUNNING, "with update");
            throw t;
        }

        Slot slot = agent.getSlot(slotStatus.getId());
        assertNotNull(slot, "slot is null");
        assertEquals(slot.status().getAssignment(), APPLE_INSTALLATION.getAssignment());
        try {
            assertEquals(slot.status().getState(), SlotLifecycleState.RUNNING);
        }
        catch (Throwable t) {
            assertEquals(slot.updateStatus().getState(), SlotLifecycleState.RUNNING, "with update");
            throw t;
        }
        assertEquals(slot.status().getResources().get("test"), (Object) 999);
    }

    private void waitForAgentToBeOnline(String agentInstanceId)
            throws InterruptedException
    {
        // wait for coordinator to mark the agent online
        HttpRemoteAgent remoteAgent = (HttpRemoteAgent) coordinator.getRemoteAgentByInstanceId(agentInstanceId);
        Preconditions.checkArgument(remoteAgent != null, "No such agent %s", agentInstanceId);
        Duration maxWait = new Duration(1, TimeUnit.SECONDS);
        while (maxWait.toMillis() > 1) {
            AgentStatus agentStatus = remoteAgent.status();
            if (Objects.equal(agentStatus.getState(), AgentLifecycleState.ONLINE)) {
                break;
            }

            maxWait = remoteAgent.waitForStateChange(agentStatus, maxWait);
        }
    }

    private void waitForAgentStatusPropagation(Agent agent)
            throws InterruptedException
    {
        coordinator.updateAllAgents();
        // Agent doesn't know it's instance-id so we need to lookup by agent id
        // Be careful that someone called waitForAgentToBeOnline or you will not find the agent
        HttpRemoteAgent remoteAgent = (HttpRemoteAgent) coordinator.getRemoteAgentByAgentId(agent.getAgentId());
        Preconditions.checkArgument(remoteAgent != null, "No remote agent for %s", agent.getAgentId());
        Duration maxWait = new Duration(1, TimeUnit.SECONDS);
        while (maxWait.toMillis() > 1) {
            AgentStatus agentStatus = remoteAgent.status();
            if (Objects.equal(agentStatus.getAgentId(), agent.getAgentId())) {
                break;
            }

            maxWait = remoteAgent.waitForStateChange(agentStatus, maxWait);
        }
    }

    private HttpUriBuilder coordinatorUriBuilder()
    {
        return uriBuilderFrom(coordinatorServer.getBaseUrl());
    }

    private void assertSuccess(JobStatus job)
    {
        assertTrue(Iterables.all(job.getSlotJobStatuses(), succeededPredicate()));
    }

    private JobStatus waitForCompletion(JobStatus job)
    {
        while (!job.getState().isDone()) {
            Request request = Request.Builder.prepareGet()
                    .setUri(coordinatorUriBuilder().appendPath("/v1/job").appendPath(job.getJobId().toString()).build())
                    .setHeader(AirshipHeaders.AIRSHIP_CURRENT_STATE, job.getVersion())
                    .setHeader(AirshipHeaders.AIRSHIP_MAX_WAIT, "200ms")
                    .build();

            job = httpClient.execute(request, createJsonResponseHandler(jobStatusCodec, Status.OK.getStatusCode(), Status.CREATED.getStatusCode()));
            System.err.println("XXXX" + job);
        }

        return job;
    }
}
