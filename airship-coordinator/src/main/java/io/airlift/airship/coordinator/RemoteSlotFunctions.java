package io.airlift.airship.coordinator;

import com.google.common.base.Function;

import java.util.UUID;

public class RemoteSlotFunctions
{
    public static Function<RemoteSlot, UUID> slotIdGetter()
    {
        return new Function<RemoteSlot, UUID>()
        {
            @Override
            public UUID apply(RemoteSlot input)
            {
                return input.getId();
            }
        };
    }
}
