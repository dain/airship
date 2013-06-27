package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Slot;
import io.airlift.airship.agent.Progress;
import io.airlift.airship.shared.job.RestartTask;

public class RestartTaskExecution
        extends TaskExecution
{
    public RestartTaskExecution(RestartTask restartTask)
    {
        super(restartTask);
    }

    @Override
    protected void runTask(Slot slot, Progress progress)
    {
        progress.reset("Restarting");
        slot.restart();
    }
}
