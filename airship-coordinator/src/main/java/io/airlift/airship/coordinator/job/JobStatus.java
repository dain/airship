package io.airlift.airship.coordinator.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.airship.shared.job.SlotJobStatus;

import java.util.List;

public class JobStatus
{
    private final JobId id;
    private final List<SlotJobStatus> slotJobStatuses;

    @JsonCreator
    public JobStatus(@JsonProperty("id") JobId id, @JsonProperty("slots") List<SlotJobStatus> slotJobStatuses)
    {
        this.id = id;
        this.slotJobStatuses = slotJobStatuses;
    }

    @JsonProperty
    public JobId getId()
    {
        return id;
    }

    @JsonProperty("slots")
    public List<SlotJobStatus> getSlotJobStatuses()
    {
        return slotJobStatuses;
    }
}
