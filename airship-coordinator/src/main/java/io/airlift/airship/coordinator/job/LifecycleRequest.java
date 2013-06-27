package io.airlift.airship.coordinator.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.airship.shared.IdAndVersion;

import java.util.List;

public class LifecycleRequest
{
    private final SlotLifecycleAction action;
    private final List<IdAndVersion> slots;

    @JsonCreator
    public LifecycleRequest(@JsonProperty("action") SlotLifecycleAction action, @JsonProperty("slots") List<IdAndVersion> slots)
    {
        this.action = action;
        this.slots = slots;
    }

    @JsonProperty
    public SlotLifecycleAction getAction()
    {
        return action;
    }

    @JsonProperty
    public List<IdAndVersion> getSlots()
    {
        return slots;
    }
}
