package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Progress;
import io.airlift.airship.agent.Slot;
import io.airlift.airship.shared.job.StopTask;

public class StopTaskExecution
        extends TaskExecution
{
    public StopTaskExecution(StopTask stopTask)
    {
        super(stopTask);
    }

    @Override
    protected void runTask(Slot slot, Progress progress)
    {
        progress.reset("Stopping");
        slot.stop();
    }
}
