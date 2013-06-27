package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Slot;
import io.airlift.airship.agent.Progress;
import io.airlift.airship.shared.job.StartTask;

public class StartTaskExecution
        extends TaskExecution
{
    public StartTaskExecution(StartTask startTask)
    {
        super(startTask);
    }

    @Override
    protected void runTask(Slot slot, Progress progress)
    {
        progress.reset("Starting");
        slot.start();
    }
}
