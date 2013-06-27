package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Slot;
import io.airlift.airship.agent.Progress;
import io.airlift.airship.shared.job.TerminateTask;

public class TerminateTaskExecution
        extends TaskExecution
{
    public TerminateTaskExecution(TerminateTask terminateTask)
    {
        super(terminateTask);
    }

    @Override
    protected void runTask(Slot slot, Progress progress)
    {
        progress.reset("Terminating");
        slot.terminate();
    }
}
