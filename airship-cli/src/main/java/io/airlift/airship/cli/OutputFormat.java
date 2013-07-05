package io.airlift.airship.cli;

import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.CoordinatorStatusRepresentation;
import io.airlift.airship.shared.SlotStatus;

public interface OutputFormat
{
    void displaySlots(Iterable<SlotStatus> slots);

    void displayAgents(Iterable<AgentStatus> agents);

    void displayCoordinators(Iterable<CoordinatorStatusRepresentation> coordinators);
}
