package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import io.airlift.airship.shared.SlotStatus;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class SlotJobStatus
{
    public static enum SlotJobState
    {
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
    private final URI self;
    private final SlotJobState state;
    private final SlotStatus slotStatus;
    private final String progressDescription;
    private final Double progressPercentage;
    private final List<TaskStatus> tasks;

    @JsonCreator
    public SlotJobStatus(
            @JsonProperty("slotJobId") SlotJobId slotJobId,
            @JsonProperty("self") URI self,
            @JsonProperty("state") SlotJobState state,
            @JsonProperty("slotStatus") SlotStatus slotStatus,
            @JsonProperty("progressDescription") String progressDescription,
            @JsonProperty("progressPercentage") Double progressPercentage,
            @JsonProperty("tasks") List<TaskStatus> tasks)
    {
        this.slotJobId = checkNotNull(slotJobId, "id is null");
        this.self = self;
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
    public SlotStatus getSlotStatus()
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

    public static Function<SlotJobStatus, SlotStatus> slotStatusGetter()
    {
        return new Function<SlotJobStatus, SlotStatus>()
        {
            @Override
            public SlotStatus apply(SlotJobStatus input)
            {
                return input.getSlotStatus();
            }
        };
    }

    public static Predicate<SlotJobStatus> isDonePredicate()
    {
        return new Predicate<SlotJobStatus>()
        {
            @Override
            public boolean apply(SlotJobStatus input)
            {
                return input.getState().isDone();
            }
        };
    }

    public static Predicate<SlotJobStatus> succeededPredicate()
    {
        return new Predicate<SlotJobStatus>()
        {
            @Override
            public boolean apply(SlotJobStatus input)
            {
                return input.getState() == SlotJobState.DONE;
            }
        };
    }

}
