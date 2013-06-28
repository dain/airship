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
package io.airlift.airship.agent;

import com.google.common.collect.ImmutableList;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.airship.shared.job.InstallTask;
import io.airlift.airship.shared.job.KillTask;
import io.airlift.airship.shared.job.RestartTask;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.StartTask;
import io.airlift.airship.shared.job.StopTask;
import io.airlift.airship.shared.job.Task;
import io.airlift.airship.shared.job.TerminateTask;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.airlift.airship.shared.InstallationHelper.APPLE_INSTALLATION;
import static io.airlift.airship.shared.InstallationHelper.BANANA_INSTALLATION;
import static io.airlift.airship.shared.SlotLifecycleState.KILLING;
import static io.airlift.airship.shared.SlotLifecycleState.RESTARTING;
import static io.airlift.airship.shared.SlotLifecycleState.RUNNING;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class TestLifecycleJobs
{
    private Agent agent;

    @BeforeMethod
    public void setup()
    {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));

        agent = new Agent(
                new AgentConfig().setSlotsDir(new File(tempDir, "slots").getAbsolutePath()),
                new HttpServerInfo(new HttpServerConfig(), new NodeInfo("test")),
                new NodeInfo("test"),
                new MockDeploymentManagerFactory(),
                new MockLifecycleManager());
        agent.start();
    }

    @Test
    public void testStateMachine()
            throws Exception
    {
        // nothing.install => stopped
        UUID slotId = executeTask(null, new InstallTask(APPLE_INSTALLATION), STOPPED);
        Slot slot = agent.getSlot(slotId);
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.start => running
        executeTask(slotId, getTask(RUNNING), RUNNING);
        assertEquals(slot.updateStatus().getState(), RUNNING);

        // running.start => running
        executeTask(slotId, getTask(RUNNING), RUNNING);
        assertEquals(slot.updateStatus().getState(), RUNNING);

        // running.stop => stopped
        executeTask(slotId, getTask(STOPPED), STOPPED);
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.stop => stopped
        executeTask(slotId, getTask(STOPPED), STOPPED);
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.restart => running
        executeTask(slotId, getTask(RESTARTING), RUNNING);
        assertEquals(slot.updateStatus().getState(), RUNNING);

        // running.restart => running
        executeTask(slotId, getTask(RESTARTING), RUNNING);
        assertEquals(slot.updateStatus().getState(), RUNNING);

        // running.kill => stopped
        executeTask(slotId, getTask(KILLING), STOPPED);
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.kill => stopped
        executeTask(slotId, getTask(KILLING), STOPPED);
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.install => stopped
        executeTask(slotId, new InstallTask(BANANA_INSTALLATION), STOPPED, BANANA_INSTALLATION);
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.terminate => terminated
        executeTask(slotId, getTask(TERMINATED), TERMINATED, BANANA_INSTALLATION);
        assertEquals(slot.updateStatus().getState(), TERMINATED);
    }

    private UUID executeTask(UUID slotId, Task task, SlotLifecycleState expectedState)
            throws InterruptedException
    {
        return executeTask(slotId, task, expectedState, APPLE_INSTALLATION);
    }

    private UUID executeTask(UUID slotId,
            Task task,
            SlotLifecycleState expectedState,
            Installation expectedInstallation)
            throws InterruptedException
    {
        SlotJob slotJob = new SlotJob(new SlotJobId("job-" + UUID.randomUUID()), slotId, ImmutableList.of(task));
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
        if (expectedState != TERMINATED) {
            assertEquals(slotStatus.getBinary(), expectedInstallation.getAssignment().getBinary());
            assertEquals(slotStatus.getConfig(), expectedInstallation.getAssignment().getConfig());
        }
        assertEquals(slotStatus.getStatus(), expectedState.toString());

        Slot slot = agent.getSlot(slotStatus.getId());
        assertNotNull(slot, "slot is null");
        if (expectedState != TERMINATED) {
            assertEquals(slot.status().getAssignment(), expectedInstallation.getAssignment());
        }
        assertEquals(slot.status().getState(), expectedState);

        return slotStatus.getId();
    }

    private Task getTask(SlotLifecycleState newState)
    {
        switch (newState) {
            case STOPPED:
                return new StopTask();
            case RUNNING:
                return new StartTask();
            case RESTARTING:
                return new RestartTask();
            case KILLING:
                return new KillTask();
            case TERMINATED:
                return new TerminateTask();
            default:
                throw new IllegalArgumentException("Unsupported lifecycle state");
        }
    }
}
