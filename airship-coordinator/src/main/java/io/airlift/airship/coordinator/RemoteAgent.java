package io.airlift.airship.coordinator;

import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.discovery.client.ServiceDescriptor;

import java.net.URI;
import java.util.List;

public interface RemoteAgent
{
    void start();

    void stop();

    AgentStatus status();

    void setInternalUri(URI uri);

    SlotStatus install(Installation installation);

    List<? extends RemoteSlot> getSlots();

    void setServiceInventory(List<ServiceDescriptor> serviceInventory);
}
