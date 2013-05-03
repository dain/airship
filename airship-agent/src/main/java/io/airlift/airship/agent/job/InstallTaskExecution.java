package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Progress;
import io.airlift.airship.agent.Slot;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.job.InstallTask;

public class InstallTaskExecution
        extends TaskExecution
{
    private final Installation installation;

    public InstallTaskExecution(InstallTask installTask)
    {
        super(installTask);
        installation = installTask.getInstallation();
    }

    public Installation getInstallation()
    {
        return installation;
    }

    @Override
    protected void runTask(Slot slot, Progress progress)
    {
        slot.assign(installation, progress);
    }
}
