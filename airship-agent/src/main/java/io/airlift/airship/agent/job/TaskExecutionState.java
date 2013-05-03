package io.airlift.airship.agent.job;

import io.airlift.airship.shared.job.Task;
import io.airlift.airship.shared.job.TaskStatus;
import io.airlift.airship.shared.job.TaskStatus.TaskState;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.airship.shared.FailureInfo.toFailure;

public class TaskExecutionState
{
    private final Task task;

    @GuardedBy("this")
    private TaskState state = TaskState.PENDING;

    @GuardedBy("this")
    private Throwable failure;

    public TaskExecutionState(Task task)
    {
        this.task = checkNotNull(task, "task is null");
    }

    public synchronized TaskState getState()
    {
        return state;
    }

    public synchronized void setState(TaskState newState)
    {
        // only update if not done
        if (state.isDone()) {
            return;
        }
        state = newState;
    }

    public synchronized void fail(Throwable throwable)
    {
        setState(TaskState.FAILED);
        if (failure == null) {
            failure = throwable;
        }
        else {
            failure.addSuppressed(throwable);
        }
    }

    public synchronized TaskStatus status()
    {
        return new TaskStatus(task, state, toFailure(failure));
    }

    @Override
    public synchronized String toString()
    {
        return state + ":" + task;
    }
}
