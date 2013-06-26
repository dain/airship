package io.airlift.airship.agent;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotStatus;

import java.net.URI;
import java.util.UUID;

public interface Slot
{
    UUID getId();

    URI getSelf();

    URI getExternalUri();

    SlotStatus terminate();

    SlotStatus assign(Installation installation, Progress progress);

    SlotStatus getLastSlotStatus();

    SlotStatus status();

    @VisibleForTesting
    SlotStatus updateStatus();

    SlotStatus start();

    SlotStatus restart();

    SlotStatus stop();

    SlotStatus kill();
}
