package io.airlift.airship.shared;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;

@Immutable
public class AgentStatus
{
    private final String agentId;
    private final AgentLifecycleState state;
    private final String instanceId;
    private final URI internalUri;
    private final URI externalUri;
    private final Map<UUID, SlotStatus> slots;
    private final String location;
    private final String instanceType;
    private final Map<String, Integer> resources;
    private final String version;

    public AgentStatus(String agentId,
            AgentLifecycleState state,
            final String instanceId,
            URI internalUri,
            URI externalUri,
            String location,
            String instanceType,
            Iterable<SlotStatus> slots,
            Map<String, Integer> resources)
    {
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(slots, "slots is null");
        Preconditions.checkNotNull(resources, "resources is null");

        this.agentId = agentId;
        this.state = state;
        this.instanceId = instanceId;
        this.internalUri = internalUri;
        this.externalUri = externalUri;
        this.location = location;
        this.instanceType = instanceType;

        slots = transform(slots, new Function<SlotStatus, SlotStatus>()
        {
            public SlotStatus apply(SlotStatus slotStatus)
            {
                if (!Objects.equal(slotStatus.getInstanceId(), instanceId)) {
                    slotStatus = slotStatus.changeInstanceId(instanceId);
                }
                return slotStatus;
            }
        });
        this.slots = Maps.uniqueIndex(slots, SlotStatus.uuidGetter());

        this.resources = ImmutableMap.copyOf(resources);
        this.version = createAgentVersion(agentId, state, slots, resources);
    }

    public String getAgentId()
    {
        return agentId;
    }

    public AgentLifecycleState getState()
    {
        return state;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public AgentStatus changeState(AgentLifecycleState state)
    {
        return new AgentStatus(agentId, state, instanceId, internalUri, externalUri, location, instanceType, slots.values(), resources);
    }

    public AgentStatus changeSlotStatus(SlotStatus slotStatus)
    {
        Map<UUID, SlotStatus> slots = newHashMap(this.slots);
        if (slotStatus.getState() != TERMINATED) {
            slots.put(slotStatus.getId(), slotStatus);
        }
        else {
            slots.remove(slotStatus.getId());
        }
        return new AgentStatus(agentId, state, instanceId, internalUri, externalUri, location, instanceType, slots.values(), resources);
    }

    public AgentStatus changeAllSlotsState(SlotLifecycleState slotState)
    {
        Map<UUID, SlotStatus> slots = newHashMap(this.slots);
        for (SlotStatus slotStatus : slots.values()) {
            // set all slots to unknown state
            slots.put(slotStatus.getId(), slotStatus.changeState(slotState));
        }
        return new AgentStatus(agentId, state, instanceId, internalUri, externalUri, location, instanceType, slots.values(), resources);
    }

    public AgentStatus changeInternalUri(URI internalUri)
    {
        return new AgentStatus(agentId, state, instanceId, internalUri, externalUri, location, instanceType, slots.values(), resources);
    }

    public URI getInternalUri()
    {
        return internalUri;
    }

    public URI getExternalUri()
    {
        return externalUri;
    }

    public String getLocation()
    {
        return location;
    }

    public String getInstanceType()
    {
        return instanceType;
    }

    public SlotStatus getSlotStatus(UUID slotId)
    {
        return slots.get(slotId);
    }

    public List<SlotStatus> getSlotStatuses()
    {
        return ImmutableList.copyOf(slots.values());
    }

    public Map<String, Integer> getResources()
    {
        return resources;
    }

    public String getVersion()
    {
        return version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(
                agentId,
                state,
                instanceId,
                internalUri,
                externalUri,
                slots,
                location,
                instanceType,
                resources,
                version);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final AgentStatus other = (AgentStatus) obj;
        return Objects.equal(this.agentId, other.agentId) &&
                Objects.equal(this.state, other.state) &&
                Objects.equal(this.instanceId, other.instanceId) &&
                Objects.equal(this.internalUri, other.internalUri) &&
                Objects.equal(this.externalUri, other.externalUri) &&
                Objects.equal(this.slots, other.slots) &&
                Objects.equal(this.location, other.location) &&
                Objects.equal(this.instanceType, other.instanceType) &&
                Objects.equal(this.resources, other.resources) &&
                Objects.equal(this.version, other.version);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("agentId", agentId)
                .add("state", state)
                .add("instanceId", instanceId)
                .add("internalUri", internalUri)
                .add("externalUri", externalUri)
                .add("slots", slots)
                .add("location", location)
                .add("instanceType", instanceType)
                .add("resources", resources)
                .add("version", version)
                .toString();
    }

    private static String createAgentVersion(String agentId, AgentLifecycleState state, Iterable<SlotStatus> slots, Map<String, Integer> resources)
    {
        List<Object> parts = new ArrayList<>();
        parts.add(agentId);
        parts.add(state);

        // canonicalize slot order
        Map<UUID, String> slotVersions = new TreeMap<>();
        for (SlotStatus slot : slots) {
            slotVersions.put(slot.getId(), slot.getVersion());
        }
        parts.addAll(slotVersions.values());

        // canonicalize resources
        parts.add(Joiner.on("--").withKeyValueSeparator("=").join(ImmutableSortedMap.copyOf(resources)));

        String data = Joiner.on("||").useForNull("--NULL--").join(parts);
        return DigestUtils.md5Hex(data);
    }

    public static Function<AgentStatus, String> idGetter()
    {
        return new Function<AgentStatus, String>()
        {
            public String apply(AgentStatus input)
            {
                return input.getAgentId();
            }
        };
    }

    public static Function<AgentStatus, String> locationGetter(final String defaultValue)
    {
        return new Function<AgentStatus, String>()
        {
            public String apply(AgentStatus input)
            {
                return firstNonNull(input.getLocation(), defaultValue);
            }
        };
    }
}
