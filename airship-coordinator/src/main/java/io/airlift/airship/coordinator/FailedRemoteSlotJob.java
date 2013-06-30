package io.airlift.airship.coordinator;

import com.google.common.collect.ImmutableList;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.Task;
import io.airlift.airship.shared.job.TaskStatus;
import io.airlift.airship.shared.job.TaskStatus.TaskState;

public class FailedRemoteSlotJob
        implements RemoteSlotJob
{
    private final SlotJobStatus slotJobStatus;

    public FailedRemoteSlotJob(SlotJob slotJob)
    {
        ImmutableList.Builder<TaskStatus> tasks = ImmutableList.builder();
        for (Task task : slotJob.getTasks()) {
            tasks.add(new TaskStatus(task, TaskState.FAILED, null));
        }

        slotJobStatus = new SlotJobStatus(slotJob.getSlotJobId(),
                null,
                SlotJobState.FAILED,
                null,
                null,
                100.0,
                tasks.build());
    }

    @Override
    public SlotJobStatus getJobStatus()
    {
        return slotJobStatus;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<SlotJobStatus> stateChangeListener)
    {
    }
}
