package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SlotJob
{
    private final SlotJobId slotJobId;

    @Nullable
    private final UUID slotId;

    private final List<Task> tasks;

    @JsonCreator
    public SlotJob(@JsonProperty("slotJobId") SlotJobId slotJobId,
            @JsonProperty("slotId") @Nullable UUID slotId,
            @JsonProperty("tasks") List<? extends Task> tasks)
    {
        this.slotJobId = checkNotNull(slotJobId, "slotJobId is null");
        this.slotId = slotId;
        this.tasks = ImmutableList.copyOf(checkNotNull(tasks, "tasks is null"));
        checkArgument(!this.tasks.isEmpty(), "tasks is empty");
    }

    @JsonProperty
    public SlotJobId getSlotJobId()
    {
        return slotJobId;
    }

    @JsonProperty
    @Nullable
    public UUID getSlotId()
    {
        return slotId;
    }

    @JsonProperty
    public List<Task> getTasks()
    {
        return tasks;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("slotJobId", slotJobId)
                .add("slotId", slotId)
                .add("tasks", tasks)
                .toString();
    }
}
