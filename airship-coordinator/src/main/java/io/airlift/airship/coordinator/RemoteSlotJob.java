package io.airlift.airship.coordinator;

import io.airlift.airship.shared.job.SlotJobStatus;

public interface RemoteSlotJob
{
    SlotJobStatus getJobStatus();
}
