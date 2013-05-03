package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.airlift.airship.shared.FailureInfo;

public class TaskStatus
{
    public static enum TaskState
    {
        PENDING(false),
        RUNNING(false),
        DONE(true),
        SKIPPED(true),
        FAILED(true);

        private final boolean done;

        private TaskState(boolean done)
        {
            this.done = done;
        }

        public boolean isDone()
        {
            return done;
        }
    }

    private final Task task;
    private final TaskState state;
    private final FailureInfo failureInfo;

    @JsonCreator
    public TaskStatus(@JsonProperty("task") Task task,
            @JsonProperty("state") TaskState state,
            @JsonProperty("failureInfo") FailureInfo failureInfo)
    {
        this.task = task;
        this.state = state;
        this.failureInfo = failureInfo;
    }

    @JsonProperty
    public Task getTask()
    {
        return task;
    }

    @JsonProperty
    public TaskState getState()
    {
        return state;
    }

    @JsonProperty
    public FailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("task", task)
                .add("state", state)
                .add("failureInfo", failureInfo)
                .toString();
    }
}
