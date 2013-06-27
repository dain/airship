package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.airlift.airship.shared.SlotStatusRepresentation;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class SlotJobStatus
{
    private final URI self;

    public static enum SlotJobState {
        PENDING(false),
        RUNNING(false),
        DONE(true),
        CANCELED(true),
        FAILED(true);

        private final boolean done;

        private SlotJobState(boolean done)
        {
            this.done = done;
        }

        public boolean isDone()
        {
            return done;
        }
    }

    private final SlotJobId slotJobId;
    private final SlotJobState state;
    private final SlotStatusRepresentation slotStatus;
    private final String progressDescription;
    private final Double progressPercentage;
    private final List<TaskStatus> tasks;

    @JsonCreator
    public SlotJobStatus(
            @JsonProperty("slotJobId") SlotJobId slotJobId,
            @JsonProperty("self") URI self,
            @JsonProperty("state") SlotJobState state,
            @JsonProperty("slotStatus") SlotStatusRepresentation slotStatus,
            @JsonProperty("progressDescription") String progressDescription,
            @JsonProperty("progressPercentage") Double progressPercentage,
            @JsonProperty("tasks") List<TaskStatus> tasks)
    {
        this.slotJobId = checkNotNull(slotJobId, "id is null");
        this.self = checkNotNull(self, "self is null");
        this.state = checkNotNull(state, "state is null");
        this.slotStatus = slotStatus;
        this.progressDescription = progressDescription;
        this.progressPercentage = progressPercentage;
        this.tasks = checkNotNull(tasks, "tasks is null");
    }

    @JsonProperty
    public SlotJobId getSlotJobId()
    {
        return slotJobId;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public SlotJobState getState()
    {
        return state;
    }

    @JsonProperty
    public SlotStatusRepresentation getSlotStatus()
    {
        return slotStatus;
    }

    @JsonProperty
    public String getProgressDescription()
    {
        return progressDescription;
    }

    @JsonProperty
    public Double getProgressPercentage()
    {
        return progressPercentage;
    }

    @JsonProperty
    public List<TaskStatus> getTasks()
    {
        return tasks;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("slotJobId", slotJobId)
                .add("self", self)
                .add("state", state)
                .add("statusDescription", progressDescription)
                .add("progress", progressPercentage)
                .add("tasks", tasks)
                .toString();
    }
}
