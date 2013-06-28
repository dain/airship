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
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.coordinator.job.LifecycleRequest;
import io.airlift.airship.coordinator.job.SlotLifecycleAction;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.MockUriInfo;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.airlift.airship.coordinator.TestingMavenRepository.MOCK_REPO;
import static io.airlift.airship.shared.AgentLifecycleState.ONLINE;
import static io.airlift.airship.shared.AssignmentHelper.APPLE_ASSIGNMENT;
import static io.airlift.airship.shared.AssignmentHelper.BANANA_ASSIGNMENT;
import static io.airlift.airship.shared.ExtraAssertions.assertEqualsNoOrder;
import static io.airlift.airship.shared.SlotLifecycleState.RUNNING;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotStatus.createSlotStatus;
import static io.airlift.airship.shared.job.SlotJobStatus.slotStatusGetter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestCoordinatorLifecycleResource
{
    private final UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/lifecycle");
    private CoordinatorLifecycleResource resource;

    private Coordinator coordinator;
    private String agentId;
    private UUID apple1SlotId;
    private UUID apple2SlotId;
    private UUID bananaSlotId;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        NodeInfo nodeInfo = new NodeInfo("testing");

        MockProvisioner provisioner = new MockProvisioner();
        coordinator = new Coordinator(nodeInfo,
                new HttpServerInfo(new HttpServerConfig(), nodeInfo),
                new CoordinatorConfig().setStatusExpiration(new Duration(1, TimeUnit.DAYS)),
                provisioner.getCoordinatorFactory(),
                provisioner.getAgentFactory(),
                MOCK_REPO,
                provisioner,
                new InMemoryStateManager(),
                new MockServiceInventory());
        resource = new CoordinatorLifecycleResource(coordinator);

        apple1SlotId = UUID.randomUUID();
        SlotStatus appleSlotStatus1 = createSlotStatus(apple1SlotId,
                URI.create("fake://foo/v1/agent/slot/apple1"),
                URI.create("fake://foo/v1/agent/slot/apple1"),
                "instance",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/apple1",
                ImmutableMap.<String, Integer>of());
        apple2SlotId = UUID.randomUUID();
        SlotStatus appleSlotStatus2 = createSlotStatus(apple2SlotId,
                URI.create("fake://foo/v1/agent/slot/apple2"),
                URI.create("fake://foo/v1/agent/slot/apple2"),
                "instance",
                "/location",
                STOPPED,
                APPLE_ASSIGNMENT,
                "/apple2",
                ImmutableMap.<String, Integer>of());
        bananaSlotId = UUID.randomUUID();
        SlotStatus bananaSlotStatus = createSlotStatus(bananaSlotId,
                URI.create("fake://foo/v1/agent/slot/banana"),
                URI.create("fake://foo/v1/agent/slot/banana"),
                "instance",
                "/location",
                STOPPED,
                BANANA_ASSIGNMENT,
                "/banana",
                ImmutableMap.<String, Integer>of());

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
    }

    @Test
    public void testMultipleStateMachineWithFilter()
    {
        UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/lifecycle");

        // default state is stopped
        assertSlotState(apple1SlotId, STOPPED);
        assertSlotState(apple2SlotId, STOPPED);
        assertSlotState(bananaSlotId, STOPPED);

        // stopped.start => running
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.START, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), RUNNING, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, RUNNING);
        assertSlotState(apple2SlotId, RUNNING);
        assertSlotState(bananaSlotId, STOPPED);

        // running.start => running
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.START, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), RUNNING, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, RUNNING);
        assertSlotState(apple2SlotId, RUNNING);
        assertSlotState(bananaSlotId, STOPPED);

        // running.stop => stopped
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.STOP, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), STOPPED, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, STOPPED);
        assertSlotState(apple2SlotId, STOPPED);
        assertSlotState(bananaSlotId, STOPPED);

        // stopped.stop => stopped
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.STOP, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), STOPPED, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, STOPPED);
        assertSlotState(apple2SlotId, STOPPED);
        assertSlotState(bananaSlotId, STOPPED);

        // stopped.restart => running
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.RESTART, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), RUNNING, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, RUNNING);
        assertSlotState(apple2SlotId, RUNNING);
        assertSlotState(bananaSlotId, STOPPED);

        // running.restart => running
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.RESTART, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), RUNNING, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, RUNNING);
        assertSlotState(apple2SlotId, RUNNING);
        assertSlotState(bananaSlotId, STOPPED);

        // running.kill => stopped
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.KILL, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), STOPPED, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, STOPPED);
        assertSlotState(apple2SlotId, STOPPED);
        assertSlotState(bananaSlotId, STOPPED);

        // stopped.kill => stopped
        assertOkResponse(resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.KILL, IdAndVersion.forIds(apple1SlotId, apple2SlotId)), uriInfo), STOPPED, apple1SlotId, apple2SlotId);
        assertSlotState(apple1SlotId, STOPPED);
        assertSlotState(apple2SlotId, STOPPED);
        assertSlotState(bananaSlotId, STOPPED);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSetStateNullState()
    {
        resource.doLifecycle(null, uriInfo);
    }

    @Test
    public void testSetStateNoFilter()
    {
        Response response = resource.doLifecycle(new LifecycleRequest(SlotLifecycleAction.START, ImmutableList.<IdAndVersion>of()), MockUriInfo.from("http://localhost/v1/slot/lifecycle"));
        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testValidVersion()
    {
//        UriInfo uriInfo = MockUriInfo.from("http://localhost/v1/slot/lifecycle?binary=*:apple:*");
//        String slotsVersion = VersionsUtil.createSlotsVersion(coordinator.getAllSlotsStatus(SlotFilterBuilder.build(uriInfo, false, ImmutableList.<UUID>of())));
//        assertOkResponse(resource.doLifecycle("running", uriInfo, slotsVersion), RUNNING, apple1SlotId, apple2SlotId);
    }

    private void assertOkResponse(Response response, SlotLifecycleState state, UUID... slotIds)
    {
        assertTrue(response.getStatus() == Response.Status.OK.getStatusCode() || response.getStatus() == Response.Status.CREATED.getStatusCode());

        JobStatus job = (JobStatus) response.getEntity();

        AgentStatus agentStatus = coordinator.getAgentByAgentId(agentId);
        Builder<SlotStatusRepresentation> builder = ImmutableList.builder();
        for (UUID slotId : slotIds) {
            SlotStatus slotStatus = agentStatus.getSlotStatus(slotId);
            builder.add(SlotStatusRepresentation.from(slotStatus.changeState(state)));
            assertEquals(slotStatus.getAssignment(), APPLE_ASSIGNMENT);
        }

        assertEqualsNoOrder(Iterables.transform(job.getSlotJobStatuses(), slotStatusGetter()), builder.build());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
    }

    private void assertSlotState(UUID slotId, SlotLifecycleState state)
    {
        assertEquals(coordinator.getAgentByAgentId(agentId).getSlotStatus(slotId).getState(), state);

    }
}
