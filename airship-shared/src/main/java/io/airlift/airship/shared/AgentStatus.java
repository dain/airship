package io.airlift.airship.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

import javax.annotation.concurrent.Immutable;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static io.airlift.airship.shared.Strings.commonPrefixSegments;
import static io.airlift.airship.shared.Strings.safeTruncate;
import static io.airlift.airship.shared.Strings.shortestUniquePrefix;
import static io.airlift.airship.shared.Strings.trimLeadingSegments;

@Immutable
public class AgentStatus
{
    private final String agentId;
    private final String shortAgentId;
    private final AgentLifecycleState state;
    private final String instanceId;
    private final URI internalUri;
    private final URI externalUri;
    private final Map<UUID, SlotStatus> slots;
    private final String location;
    private final String instanceType;
    private final String shortLocation;
    private final Map<String, Integer> resources;
    private final String version;

    public AgentStatus(
            String agentId,
            AgentLifecycleState state,
            String instanceId,
            URI internalUri,
            URI externalUri,
            String location,
            String instanceType,
            Collection<SlotStatus> slots,
            Map<String, Integer> resources)
    {
        this(agentId,
                agentId,
                state,
                instanceId,
                internalUri,
                externalUri,
                location,
                location,
                instanceType,
                slots,
                resources,
                createAgentVersion(agentId, state, slots, resources));
    }

    @JsonCreator
    public AgentStatus(
            @JsonProperty("agentId") String agentId,
            @JsonProperty("shortAgentId") String shortAgentId,
            @JsonProperty("state") AgentLifecycleState state,
            @JsonProperty("instanceId") final String instanceId,
            @JsonProperty("internalUri") URI internalUri,
            @JsonProperty("externalUri") URI externalUri,
            @JsonProperty("location") String location,
            @JsonProperty("shortLocation") String shortLocation,
            @JsonProperty("instanceType") String instanceType,
            @JsonProperty("slots") Collection<SlotStatus> slots,
            @JsonProperty("resources") Map<String, Integer> resources,
            @JsonProperty("version") String version)
    {
        // agent id is null while agent is offline
        this.agentId = agentId;
        this.shortAgentId = shortAgentId;
        this.state = checkNotNull(state, "state is null");
        this.instanceId = instanceId;
        this.internalUri = internalUri;
        this.externalUri = externalUri;
        this.location = location;
        this.shortLocation = shortLocation;
        this.instanceType = instanceType;

        slots = ImmutableList.copyOf(transform(slots, new Function<SlotStatus, SlotStatus>()
        {
            public SlotStatus apply(SlotStatus slotStatus)
            {
                if (!Objects.equal(slotStatus.getInstanceId(), instanceId)) {
                    slotStatus = slotStatus.changeInstanceId(instanceId);
                }
                return slotStatus;
            }
        }));
        this.slots = Maps.uniqueIndex(slots, SlotStatus.uuidGetter());

        this.resources = ImmutableMap.copyOf(checkNotNull(resources, "resources is null"));
        this.version = version;
    }

    @JsonProperty
    public String getAgentId()
    {
        return agentId;
    }

    @JsonProperty
    public String getShortAgentId()
    {
        return shortAgentId;
    }

    @JsonProperty
    public AgentLifecycleState getState()
    {
        return state;
    }

    @JsonProperty
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

    public AgentStatus changeInstance(String instanceId, String instanceType)
    {
        return new AgentStatus(agentId, state, instanceId, internalUri, externalUri, location, instanceType, slots.values(), resources);
    }

    @JsonProperty
    public URI getInternalUri()
    {
        return internalUri;
    }

    public String getInternalHost()
    {
        if (getInternalUri() == null) {
            return null;
        }
        return getInternalUri().getHost();
    }

    public String getInternalIp()
    {
        String host = getInternalHost();
        if (host == null) {
            return null;
        }

        if ("localhost".equalsIgnoreCase(host)) {
            return "127.0.0.1";
        }

        try {
            return InetAddress.getByName(host).getHostAddress();
        }
        catch (UnknownHostException e) {
            return "unknown";
        }
    }

    @JsonProperty
    public URI getExternalUri()
    {
        return externalUri;
    }

    public String getExternalHost()
    {
        if (getExternalUri() == null) {
            return null;
        }
        return getExternalUri().getHost();
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public String getShortLocation()
    {
        return shortLocation;
    }

    @JsonProperty
    public String getInstanceType()
    {
        return instanceType;
    }

    public SlotStatus getSlot(UUID slotId)
    {
        return slots.get(slotId);
    }

    @JsonProperty
    public List<SlotStatus> getSlots()
    {
        return ImmutableList.copyOf(slots.values());
    }

    @JsonProperty
    public Map<String, Integer> getResources()
    {
        return resources;
    }

    @JsonProperty
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
                .add("shortAgentId", shortAgentId)
                .add("state", state)
                .add("instanceId", instanceId)
                .add("internalUri", internalUri)
                .add("externalUri", externalUri)
                .add("slots", slots)
                .add("location", location)
                .add("shortLocation", shortLocation)
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

    public static Function<AgentStatus, IdAndVersion> agentToIdWithVersion()
    {
        return new Function<AgentStatus, IdAndVersion>()
        {
            @Override
            public IdAndVersion apply(AgentStatus input)
            {
                return new IdAndVersion(input.getAgentId(), input.getVersion());
            }
        };
    }

    public static Function<AgentStatus, IdAndVersion> agentToIdWithoutVersion()
    {
        return new Function<AgentStatus, IdAndVersion>()
        {
            @Override
            public IdAndVersion apply(AgentStatus input)
            {
                return new IdAndVersion(input.getAgentId(), null);
            }
        };
    }

    public static Function<AgentStatus, AgentStatus> shortenAgentStatus(List<AgentStatus> agentStatuses)
    {
        return shortenAgentStatus(new AgentStatusFactory(agentStatuses));
    }

    public static Function<AgentStatus, AgentStatus> shortenAgentStatus(final AgentStatusFactory factory)
    {
        return new Function<AgentStatus, AgentStatus>()
        {
            public AgentStatus apply(AgentStatus status)
            {
                return factory.create(status);
            }
        };
    }

    public static class AgentStatusFactory
    {
        public static final int MIN_PREFIX_SIZE = 4;
        public static final int MIN_LOCATION_SEGMENTS = 2;

        private final int shortIdPrefixSize;
        private final int commonLocationParts;

        public AgentStatusFactory(List<AgentStatus> agentStatuses)
        {
            this.shortIdPrefixSize = shortestUniquePrefix(transform(agentStatuses, idGetter()), MIN_PREFIX_SIZE);
            this.commonLocationParts = commonPrefixSegments('/', transform(agentStatuses, locationGetter("/")), MIN_LOCATION_SEGMENTS);
        }

        public AgentStatusFactory(int shortIdPrefixSize, int commonLocationParts)
        {
            this.shortIdPrefixSize = shortIdPrefixSize;
            this.commonLocationParts = commonLocationParts;
        }

        public AgentStatus create(AgentStatus status)
        {
            return new AgentStatus(
                    status.getAgentId(),
                    safeTruncate(status.getAgentId(), shortIdPrefixSize),
                    status.getState(),
                    status.getInstanceId(),
                    status.getInternalUri(),
                    status.getExternalUri(),
                    status.getLocation(),
                    trimLeadingSegments(status.getLocation(), '/', commonLocationParts),
                    status.getInstanceType(),
                    status.getSlots(),
                    status.getResources(),
                    status.getVersion());
        }
    }
}
