package io.airlift.airship.cli;

import io.airlift.airship.coordinator.job.SlotLifecycleAction;
import io.airlift.airship.shared.AgentStatusRepresentation;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.CoordinatorStatusRepresentation;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.SlotStatusRepresentation;

import java.util.List;

public interface Commander
{
    List<SlotStatusRepresentation> show(SlotFilter slotFilter);

    RemoteJob install(List<IdAndVersion> agents, Assignment assignment);

    RemoteJob upgrade(List<IdAndVersion> slots, Assignment assignment, boolean force);

    RemoteJob doLifecycle(List<IdAndVersion> slots, SlotLifecycleAction state);

    RemoteJob terminate(List<IdAndVersion> slots);

    RemoteJob resetExpectedState(List<IdAndVersion> slots);

    boolean ssh(SlotFilter slotFilter, String command);

    List<CoordinatorStatusRepresentation> showCoordinators(CoordinatorFilter coordinatorFilter);

    RemoteJob provisionCoordinators(String coordinatorConfig,
            int coordinatorCount,
            String instanceType,
            String availabilityZone,
            String ami,
            String keyPair,
            String securityGroup,
            boolean waitForStartup);

    boolean sshCoordinator(CoordinatorFilter coordinatorFilter, String command);

    List<AgentStatusRepresentation> showAgents(AgentFilter agentFilter);

    RemoteJob provisionAgents(String agentConfig,
            int agentCount,
            String instanceType,
            String availabilityZone,
            String ami,
            String keyPair,
            String securityGroup,
            boolean waitForStartup);

    RemoteJob terminateAgent(String agentId);

    boolean sshAgent(AgentFilter agentFilter, String command);
}
