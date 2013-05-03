package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Progress;
import io.airlift.airship.agent.Slot;
import io.airlift.airship.shared.job.KillTask;

public class KillTaskExecution
        extends TaskExecution
{
    public KillTaskExecution(KillTask killTask)
    {
        super(killTask);
    }

    @Override
    protected void runTask(Slot slot, Progress progress)
    {
        progress.reset("Killing");
        slot.kill();
    }
}
