package io.airlift.airship.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.airship.coordinator.job.InstallationRequest;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.ExpectedSlotStatus;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.MockUriInfo;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.airlift.airship.coordinator.CoordinatorSlotResource.MIN_PREFIX_SIZE;
import static io.airlift.airship.shared.AgentLifecycleState.ONLINE;
import static io.airlift.airship.shared.AssignmentHelper.APPLE_ASSIGNMENT;
import static io.airlift.airship.shared.ExtraAssertions.assertEqualsNoOrder;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotStatus.shortenSlotStatus;
import static io.airlift.airship.shared.Strings.shortestUniquePrefix;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestCoordinatorSlotResource
{
    private CoordinatorSlotResource resource;
    private Coordinator coordinator;
    private TestingMavenRepository repository;
    private MockProvisioner provisioner;
    private InMemoryStateManager stateManager;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        NodeInfo nodeInfo = new NodeInfo("testing");

        repository = new TestingMavenRepository();

        provisioner = new MockProvisioner();
        stateManager = new InMemoryStateManager();
        coordinator = new Coordinator(nodeInfo,
                new HttpServerInfo(new HttpServerConfig(), nodeInfo),
                new CoordinatorConfig().setStatusExpiration(new Duration(1, TimeUnit.DAYS)),
                provisioner.getCoordinatorFactory(),
                provisioner.getAgentFactory(),
                repository,
                provisioner,
                stateManager,
                new MockServiceInventory());
        resource = new CoordinatorSlotResource(coordinator, repository);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        repository.destroy();
    }

    @Test
    public void testGetAllSlots()
    {
        UUID slot1Id = UUID.randomUUID();
        SlotStatus slot1 = new SlotStatus(
                slot1Id,
                slot1Id.toString(),
                URI.create("fake://localhost/v1/agent/slot/slot1"),
                URI.create("fake://localhost/v1/agent/slot/slot1"),
                "instance-id",
                "/location",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/slot1",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                APPLE_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(slot1Id, STOPPED, APPLE_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(slot1));

        UUID slot2Id = UUID.randomUUID();
        SlotStatus slot2 = new SlotStatus(
                slot2Id,
                slot2Id.toString(),
                URI.create("fake://localhost/v1/agent/slot/slot2"),
                URI.create("fake://localhost/v1/agent/slot/slot2"),
                "instance-id",
                "/location",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/slot2",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                APPLE_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(slot2Id, STOPPED, APPLE_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(slot2));

        AgentStatus agentStatus = new AgentStatus(UUID.randomUUID().toString(),
                ONLINE,
                "instance-id",
                URI.create("fake://foo/"),
                URI.create("fake://foo/"),
                "/unknown/location",
                "instance.type",
                ImmutableList.of(slot1, slot2),
                ImmutableMap.<String, Integer>of());
        provisioner.addAgents(agentStatus);
        coordinator.updateAllAgents();

        int prefixSize = shortestUniquePrefix(asList(slot1.getId().toString(), slot2.getId().toString()), MIN_PREFIX_SIZE);

        URI requestUri = URI.create("http://localhost/v1/slot");
        Response response = resource.getAllSlots(MockUriInfo.from(requestUri));
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEqualsNoOrder((Iterable<?>) response.getEntity(),
                ImmutableList.of(shortenSlotStatus(slot1, prefixSize, 0, repository), shortenSlotStatus(slot2, prefixSize, 0, repository)));
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
    }

    @Test
    public void testGetAllSlotsWithFilter()
    {
        UUID slot1Id = UUID.randomUUID();
        SlotStatus slot1 = new SlotStatus(
                slot1Id,
                slot1Id.toString(),
                URI.create("fake://foo/v1/agent/slot/slot1"),
                URI.create("fake://foo/v1/agent/slot/slot1"),
                "instance-id",
                "/location",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/slot1",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                APPLE_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(slot1Id, STOPPED, APPLE_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(slot1));

        UUID slot2Id = UUID.randomUUID();
        SlotStatus slot2 = new SlotStatus(
                slot2Id,
                slot2Id.toString(),
                URI.create("fake://bar/v1/agent/slot/slot2"),
                URI.create("fake://bar/v1/agent/slot/slot2"),
                "instance-id",
                "/location",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/slot2",
                ImmutableMap.<String, Integer>of(),
                STOPPED,
                APPLE_ASSIGNMENT,
                null,
                SlotStatus.createSlotVersion(slot2Id, STOPPED, APPLE_ASSIGNMENT));
        stateManager.setExpectedState(new ExpectedSlotStatus(slot2));

        AgentStatus agentStatus = new AgentStatus(UUID.randomUUID().toString(),
                ONLINE,
                "instance-id",
                URI.create("fake://foo/"),
                URI.create("fake://foo/"),
                "/unknown/location",
                "instance.type",
                ImmutableList.of(slot1, slot2),
                ImmutableMap.<String, Integer>of());
        provisioner.addAgents(agentStatus);
        coordinator.updateAllAgents();

        int prefixSize = shortestUniquePrefix(asList(slot1.getId().toString(), slot2.getId().toString()), MIN_PREFIX_SIZE);

        URI requestUri = URI.create("http://localhost/v1/slot?host=foo");
        Response response = resource.getAllSlots(MockUriInfo.from(requestUri));
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEqualsNoOrder((Iterable<?>) response.getEntity(), ImmutableList.of(shortenSlotStatus(slot1, prefixSize, 0, repository)));
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
    }

    @Test
    public void testGetAllSlotEmpty()
    {
        URI requestUri = URI.create("http://localhost/v1/slot?state=unknown");
        Response response = resource.getAllSlots(MockUriInfo.from(requestUri));
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEqualsNoOrder((Iterable<?>) response.getEntity(), ImmutableList.of());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
    }

    @Test
    public void testInstallOne()
    {
        testInstall(1, APPLE_ASSIGNMENT);
    }

    @Test
    public void testInstallMultiple()
    {
        testInstall(10, APPLE_ASSIGNMENT);
    }

    public void testInstall(int numberOfAgents, Assignment assignment)
    {
        List<IdAndVersion> agentIds = new ArrayList<>();
        for (int i = 0; i < numberOfAgents; i++) {
            UUID uuid = UUID.randomUUID();
            agentIds.addAll(IdAndVersion.forIds(uuid));
            provisioner.addAgent(uuid.toString(), URI.create("fake://appleServer1/"), ImmutableMap.of("cpu", 8, "memory", 1024));
        }
        coordinator.updateAllAgents();

        UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/assignment");
        Response response = resource.install(new InstallationRequest(assignment, agentIds), uriInfo);

        assertTrue(response.getStatus() == Response.Status.OK.getStatusCode() || response.getStatus() == Response.Status.CREATED.getStatusCode());

        JobStatus job = (JobStatus) response.getEntity();
        assertEquals(job.getSlotJobStatuses().size(), numberOfAgents);
        for (SlotJobStatus slotJob : job.getSlotJobStatuses()) {
            SlotStatus slot = slotJob.getSlotStatus().changeInstanceId("instance");
            assertEquals(slot.getAssignment(), assignment);
            assertEquals(slot.getState(), STOPPED);
        }

        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
    }

//    @Test
//    public void testInstallWithinResourceLimit()
//    {
//        UUID agentId = UUID.randomUUID();
//        provisioner.addAgent(agentId.toString(), URI.create("fake://appleServer1/"), ImmutableMap.of("cpu", 1, "memory", 512));
//        coordinator.updateAllAgents();
//
//        UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/assignment");
//        Response response = resource.install(new InstallationRequest(APPLE_ASSIGNMENT, IdAndVersion.forIds(agentId)), uriInfo);
//
//        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
//
//        Collection<SlotStatusRepresentation> slots = (Collection<SlotStatusRepresentation>) response.getEntity();
//        assertEquals(slots.size(), 1);
//        for (SlotStatusRepresentation slotRepresentation : slots) {
//            assertAppleSlot(slotRepresentation);
//        }
//
//        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
//    }

//    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No agents have the available resources to run the specified binary and configuration.")
//    public void testInstallNotEnoughResources()
//    {
//        UUID agentId = UUID.randomUUID();
//        provisioner.addAgent(agentId.toString(), URI.create("fake://appleServer1/"), ImmutableMap.of("cpu", 0, "memory", 0));
//        coordinator.updateAllAgents();
//
//        UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/assignment");
//        Response response = resource.install(new InstallationRequest(APPLE_ASSIGNMENT, IdAndVersion.forIds(agentId)), uriInfo);
//
//        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
//
//        response.getEntity();
//    }

//    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No agents have the available resources to run the specified binary and configuration.")
//    public void testInstallResourcesConsumed()
//    {
//        UUID agentId = UUID.randomUUID();
//        provisioner.addAgent(agentId.toString(), URI.create("fake://appleServer1/"), ImmutableMap.of("cpu", 1, "memory", 512));
//        coordinator.updateAllAgents();
//
//        UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/assignment");
//
//        // install an apple server
//        Response response = resource.install(new InstallationRequest(APPLE_ASSIGNMENT, IdAndVersion.forIds(agentId)), uriInfo);
//        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
//        Collection<SlotStatusRepresentation> slots = (Collection<SlotStatusRepresentation>) response.getEntity();
//        assertEquals(slots.size(), 1);
//        assertAppleSlot(Iterables.get(slots, 0));
//
//        // try to install a banana server which will fail
//        response = resource.install(new InstallationRequest(BANANA_ASSIGNMENT, ImmutableList.<IdAndVersion>of()), uriInfo);
//        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
//        response.getEntity();
//    }

    private void assertAppleSlot(SlotStatus slotRepresentation)
    {
        SlotStatus slot = slotRepresentation.changeInstanceId("instance");
        assertEquals(slot.getAssignment(), APPLE_ASSIGNMENT);
        assertEquals(slot.getState(), STOPPED);
        assertEquals(slot.getResources(), ImmutableMap.of("cpu", 1, "memory", 512));
    }
}
