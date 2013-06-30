package io.airlift.airship.coordinator;

import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.SlotJobStatus;

public interface RemoteSlotJob
{
    void addStateChangeListener(StateChangeListener<SlotJobStatus> stateChangeListener);

    SlotJobStatus getJobStatus();
}
