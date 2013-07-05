package io.airlift.airship.agent;

import com.google.common.collect.ImmutableList;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
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
import org.testng.annotations.Test;

import java.util.UUID;

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

public abstract class AbstractTestLifecycleJobs
{
    public abstract Agent getAgent();

    protected abstract SlotJobStatus createJob(SlotJob slotJob);

    protected abstract SlotJobStatus updateSlotJobStatus(SlotJob slotJob, SlotJobState currentState)
            throws InterruptedException;

    private UUID executeTask(UUID slotId, Task task, SlotLifecycleState expectedState)
            throws InterruptedException
    {
        return executeTask(slotId, task, expectedState, getInitialInstallation());
    }

    private UUID executeTask(UUID slotId,
            Task task,
            SlotLifecycleState expectedState,
            Installation expectedInstallation)
            throws InterruptedException
    {
        SlotJob slotJob = new SlotJob(new SlotJobId("job-" + UUID.randomUUID()), slotId, ImmutableList.of(task));
        SlotJobStatus slotJobStatus = createJob(slotJob);

        // wait for job to start
        if (slotJobStatus.getState() == SlotJobState.PENDING) {
            slotJobStatus = updateSlotJobStatus(slotJob, SlotJobState.PENDING);
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        assertNotEquals(slotJobStatus.getState(), SlotJobState.PENDING);

        // wait for job to finish
        if (slotJobStatus.getState() == SlotJobState.RUNNING) {
            slotJobStatus = updateSlotJobStatus(slotJob, SlotJobState.RUNNING);
        }
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        assertNotEquals(slotJobStatus.getState(), SlotJobState.RUNNING);

        // verify done
        assertEquals(slotJobStatus.getState(), SlotJobState.DONE);
        SlotStatus slotStatus = slotJobStatus.getSlotStatus();
        assertNotNull(slotStatus, "slotStatus is null");
        if (expectedState != TERMINATED) {
            assertEquals(slotStatus.getAssignment().getBinary(), expectedInstallation.getAssignment().getBinary());
            assertEquals(slotStatus.getAssignment().getConfig(), expectedInstallation.getAssignment().getConfig());
        }
        assertEquals(slotStatus.getState(), expectedState);

        Slot slot = getAgent().getSlot(slotStatus.getId());
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

    @Test
    public void testStateMachine()
            throws Exception
    {
        // nothing.install => stopped
        UUID slotId = executeTask(null, new InstallTask(getInitialInstallation()), STOPPED);
        Slot slot = getAgent().getSlot(slotId);
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
        executeTask(slotId, new InstallTask(getUpgradeInstallation()), STOPPED, getUpgradeInstallation());
        assertEquals(slot.updateStatus().getState(), STOPPED);

        // stopped.terminate => terminated
        executeTask(slotId, getTask(TERMINATED), TERMINATED, getUpgradeInstallation());
        assertEquals(slot.updateStatus().getState(), TERMINATED);
    }

    protected Installation getUpgradeInstallation()
    {
        return BANANA_INSTALLATION;
    }

    protected Installation getInitialInstallation()
    {
        return APPLE_INSTALLATION;
    }
}
