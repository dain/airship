package io.airlift.airship.agent.job;

import com.google.common.base.Throwables;
import io.airlift.airship.agent.Slot;
import io.airlift.airship.agent.Progress;
import io.airlift.airship.shared.job.Task;
import io.airlift.airship.shared.job.TaskStatus;
import io.airlift.airship.shared.job.TaskStatus.TaskState;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class TaskExecution
{
    private final TaskExecutionState state;

    public TaskExecution(Task task)
    {
        state = new TaskExecutionState(checkNotNull(task, "task is null"));
    }

    protected abstract void runTask(Slot slot, Progress progress);

    public final TaskStatus getStatus()
    {
        return state.status();
    }

    public final void run(Slot slot, Progress progress)
    {
        try {
            state.setState(TaskState.RUNNING);

            runTask(slot, progress);
        }
        catch (Throwable throwable) {
            state.fail(throwable);
            Throwables.propagateIfInstanceOf(throwable, Error.class);
        }
        finally {
            state.setState(TaskState.DONE);
        }
    }

    public final void skip()
    {
        state.setState(TaskState.SKIPPED);
    }

    public final String toString()
    {
        return state.toString();
    }
}
