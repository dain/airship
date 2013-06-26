package io.airlift.airship.agent;

import com.google.common.collect.ImmutableList;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.airship.shared.job.InstallTask;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.StartTask;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static io.airlift.airship.agent.ResourcesUtil.TEST_RESOURCES;
import static io.airlift.airship.shared.InstallationHelper.APPLE_INSTALLATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class TestAgent
{
    private Agent agent;
    private NodeInfo nodeInfo;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));

        File resourcesFile = new File(tempDir, "slots/resources.properties");
        ResourcesUtil.writeResources(TEST_RESOURCES, resourcesFile);

        AgentConfig config = new AgentConfig()
                .setSlotsDir(new File(tempDir, "slots").getAbsolutePath())
                .setResourcesFile(resourcesFile.getAbsolutePath())
                .setUpdateInterval(new Duration(0, TimeUnit.MILLISECONDS));

        nodeInfo = new NodeInfo("test", "pool", "nodeId", InetAddress.getByAddress(new byte[]{127, 0, 0, 1}), null, null, "location", "binarySpec", "configSpec");

        agent = new Agent(
                config,
                new HttpServerInfo(new HttpServerConfig(), nodeInfo),
                nodeInfo,
                new MockDeploymentManagerFactory(),
                new MockLifecycleManager()
        );
    }

    @Test
    public void test()
    {
        assertEquals(agent.getAgentId(), nodeInfo.getNodeId());
        assertEquals(agent.getLocation(), agent.getLocation());
        assertEquals(agent.getResources(), TEST_RESOURCES);
    }

    @Test
    public void testSlotInstallationJob()
            throws Exception
    {
        SlotJob slotJob = new SlotJob(new SlotJobId("job"), null, ImmutableList.of(new InstallTask(APPLE_INSTALLATION)));
        SlotJobStatus slotJobStatus = agent.createJob(slotJob);
        assertNotNull(slotJobStatus, "slotJobStatus is null");

        // wait for job to start
        if (slotJobStatus.getState() == SlotJobState.PENDING) {
            agent.waitForJobStateChange(slotJob.getSlotJobId(), SlotJobState.PENDING, new Duration(1, TimeUnit.MINUTES));
            slotJobStatus = agent.getJobStatus(slotJob.getSlotJobId());
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        assertNotEquals(slotJobStatus.getState(), SlotJobState.PENDING);

        // wait for job to finish
        if (slotJobStatus.getState() == SlotJobState.RUNNING) {
            agent.waitForJobStateChange(slotJob.getSlotJobId(), SlotJobState.RUNNING, new Duration(1, TimeUnit.MINUTES));
            slotJobStatus = agent.getJobStatus(slotJob.getSlotJobId());
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        assertNotEquals(slotJobStatus.getState(), SlotJobState.RUNNING);

        // verify done
        assertEquals(slotJobStatus.getState(), SlotJobState.DONE);
        SlotStatusRepresentation slotStatus = slotJobStatus.getSlotStatus();
        assertNotNull(slotStatus, "slotStatus is null");
        assertEquals(slotStatus.getBinary(), APPLE_INSTALLATION.getAssignment().getBinary());
        assertEquals(slotStatus.getConfig(), APPLE_INSTALLATION.getAssignment().getConfig());
        assertEquals(slotStatus.getStatus(), SlotLifecycleState.STOPPED.toString());

        Slot slot = agent.getSlot(slotStatus.getId());
        assertNotNull(slot, "slot is null");
        assertEquals(slot.getLastSlotStatus().getAssignment(), APPLE_INSTALLATION.getAssignment());
        assertEquals(slot.getLastSlotStatus().getState(), SlotLifecycleState.STOPPED);
    }

    @Test
    public void testSlotInstallationAndStartJob()
            throws Exception
    {
        SlotJob slotJob = new SlotJob(new SlotJobId("job"), null, ImmutableList.of(new InstallTask(APPLE_INSTALLATION), new StartTask()));
        SlotJobStatus slotJobStatus = agent.createJob(slotJob);
        assertNotNull(slotJobStatus, "slotJobStatus is null");

        // wait for job to start
        if (slotJobStatus.getState() == SlotJobState.PENDING) {
            agent.waitForJobStateChange(slotJob.getSlotJobId(), SlotJobState.PENDING, new Duration(1, TimeUnit.MINUTES));
            slotJobStatus = agent.getJobStatus(slotJob.getSlotJobId());
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        assertNotEquals(slotJobStatus.getState(), SlotJobState.PENDING);

        // wait for job to finish
        if (slotJobStatus.getState() == SlotJobState.RUNNING) {
            agent.waitForJobStateChange(slotJob.getSlotJobId(), SlotJobState.RUNNING, new Duration(1, TimeUnit.MINUTES));
            slotJobStatus = agent.getJobStatus(slotJob.getSlotJobId());
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        assertNotEquals(slotJobStatus.getState(), SlotJobState.RUNNING);

        // verify done
        assertEquals(slotJobStatus.getState(), SlotJobState.DONE);
        SlotStatusRepresentation slotStatus = slotJobStatus.getSlotStatus();
        assertNotNull(slotStatus, "slotStatus is null");
        assertEquals(slotStatus.getBinary(), APPLE_INSTALLATION.getAssignment().getBinary());
        assertEquals(slotStatus.getConfig(), APPLE_INSTALLATION.getAssignment().getConfig());
        try {
            assertEquals(slotStatus.getStatus(), SlotLifecycleState.RUNNING.toString());
        }
        catch (Throwable t) {
            assertEquals(slotStatus.getStatus(), SlotLifecycleState.RUNNING.toString(), "with update");
            throw t;
        }

        Slot slot = agent.getSlot(slotStatus.getId());
        assertNotNull(slot, "slot is null");
        assertEquals(slot.getLastSlotStatus().getAssignment(), APPLE_INSTALLATION.getAssignment());
        try {
            assertEquals(slot.getLastSlotStatus().getState(), SlotLifecycleState.RUNNING);
        }
        catch (Throwable t) {
            assertEquals(slot.updateStatus().getState(), SlotLifecycleState.RUNNING, "with update");
            throw t;
        }
    }
}
