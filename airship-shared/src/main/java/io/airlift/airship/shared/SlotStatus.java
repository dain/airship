/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.airship.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.transform;
import static io.airlift.airship.shared.Assignment.shortenAssignment;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static io.airlift.airship.shared.SlotLifecycleState.UNKNOWN;
import static io.airlift.airship.shared.Strings.commonPrefixSegments;
import static io.airlift.airship.shared.Strings.safeTruncate;
import static io.airlift.airship.shared.Strings.shortestUniquePrefix;
import static io.airlift.airship.shared.Strings.trimLeadingSegments;

@Immutable
public class SlotStatus
{
    public static SlotStatus createSlotStatus(UUID id,
            URI self,
            URI externalUri,
            String instanceId,
            String location,
            SlotLifecycleState state,
            Assignment assignment,
            String installPath,
            Map<String, Integer> resources)
    {
        return new SlotStatus(
                id,
                id.toString(),
                self,
                externalUri,
                instanceId,
                location,
                location,
                state,
                assignment,
                installPath,
                resources,
                null,
                null,
                null,
                createSlotVersion(id, state, assignment));
    }

    public static SlotStatus createSlotStatusWithExpectedState(UUID id,
            URI self,
            URI externalUri,
            String instanceId,
            String location,
            SlotLifecycleState state,
            Assignment assignment,
            String installPath,
            Map<String, Integer> resources,
            SlotLifecycleState expectedState,
            Assignment expectedAssignment,
            String statusMessage)
    {
        return new SlotStatus(
                id,
                id.toString(),
                self,
                externalUri,
                instanceId,
                location,
                location,
                state,
                assignment,
                installPath,
                resources,
                expectedState,
                expectedAssignment,
                statusMessage,
                createSlotVersion(id, state, assignment));
    }

    private final UUID id;
    private final String shortId;
    private final URI self;
    private final URI externalUri;
    private final String instanceId;
    private final String location;
    private final String shortLocation;
    private final Assignment assignment;
    private final SlotLifecycleState state;
    private final String version;

    private final SlotLifecycleState expectedState;
    private final Assignment expectedAssignment;

    private final String statusMessage;

    private final String installPath;

    private final Map<String, Integer> resources;

    public SlotStatus(UUID id,
            URI self,
            URI externalUri,
            String instanceId, String location,
            SlotLifecycleState state,
            Assignment assignment,
            String installPath,
            Map<String, Integer> resources,
            SlotLifecycleState expectedState,
            Assignment expectedAssignment,
            String statusMessage)
    {
        this(id,
                id.toString(),
                self,
                externalUri,
                instanceId,
                location,
                location,
                state,
                assignment,
                installPath,
                resources,
                expectedState,
                expectedAssignment,
                statusMessage,
                createSlotVersion(id, state, assignment));
    }

    @JsonCreator
    public SlotStatus(
            @JsonProperty("id") UUID id,
            @JsonProperty("shortId") String shortId,
            @JsonProperty("self") URI self,
            @JsonProperty("externalUri") URI externalUri,
            @JsonProperty("instanceId") String instanceId,
            @JsonProperty("location") String location,
            @JsonProperty("shortLocation") String shortLocation,
            @JsonProperty("state") SlotLifecycleState state,
            @JsonProperty("assignment") Assignment assignment,
            @JsonProperty("installPath") String installPath,
            @JsonProperty("resources") Map<String, Integer> resources,
            @JsonProperty("expectedState") SlotLifecycleState expectedState,
            @JsonProperty("expectedAssignment") Assignment expectedAssignment,
            @JsonProperty("statusMessage") String statusMessage,
            @JsonProperty("version") String version)
    {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkArgument(location.startsWith("/"), "location must start with a /");
        Preconditions.checkNotNull(state, "state is null");
        if (state != TERMINATED && state != UNKNOWN) {
            Preconditions.checkNotNull(assignment, "assignment is null");
        }
        Preconditions.checkNotNull(resources, "resources is null");

        this.id = id;
        this.shortId = shortId;
        this.self = self;
        this.externalUri = externalUri;
        this.instanceId = instanceId;
        this.location = location;
        this.shortLocation = shortLocation;
        this.assignment = assignment;
        this.state = state;
        this.installPath = installPath;
        this.expectedState = expectedState;
        this.expectedAssignment = expectedAssignment;
        this.statusMessage = statusMessage;
        this.resources = ImmutableMap.copyOf(resources);
        this.version = version;
    }

    @JsonProperty
    public UUID getId()
    {
        return id;
    }

    @JsonProperty
    public String getShortId()
    {
        return shortId;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    public String getInternalHost()
    {
        if (self == null) {
            return null;
        }
        return self.getHost();
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
        if (externalUri == null) {
            return null;
        }
        return externalUri.getHost();
    }

    @JsonProperty
    public String getInstanceId()
    {
        return instanceId;
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
    public Assignment getAssignment()
    {
        return assignment;
    }

    @JsonProperty
    public SlotLifecycleState getState()
    {
        return state;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @JsonProperty
    public SlotLifecycleState getExpectedState()
    {
        return expectedState;
    }

    @JsonProperty
    public Assignment getExpectedAssignment()
    {
        return expectedAssignment;
    }

    @JsonProperty
    public String getStatusMessage()
    {
        return statusMessage;
    }

    @JsonProperty
    public String getInstallPath()
    {
        return installPath;
    }

    @JsonProperty
    public Map<String, Integer> getResources()
    {
        return resources;
    }

    public SlotStatus changeInstanceId(String instanceId)
    {
        return new SlotStatus(
                this.id,
                this.shortId,
                this.self,
                this.externalUri,
                instanceId,
                this.location,
                this.shortLocation,
                this.state,
                this.assignment,
                this.installPath,
                this.resources,
                this.expectedState,
                this.expectedAssignment,
                this.statusMessage,
                this.version);
    }

    public SlotStatus changeState(SlotLifecycleState state)
    {
        return changeAssignment(state, getAssignment(), getResources());
    }

    public SlotStatus changeAssignment(SlotLifecycleState state, Assignment assignment, Map<String, Integer> resources)
    {
        Assignment newAssignment = state == TERMINATED ? null : assignment;
        return new SlotStatus(
                this.id,
                this.getShortId(),
                this.self,
                this.externalUri,
                this.instanceId,
                this.location,
                this.shortLocation,
                state,
                newAssignment,
                state == TERMINATED ? null : this.installPath,
                state == TERMINATED ? ImmutableMap.<String, Integer>of() : ImmutableMap.copyOf(resources),
                this.expectedState,
                this.expectedAssignment,
                this.statusMessage,
                createSlotVersion(id, state, newAssignment));
    }

    public SlotStatus changeExpectedState(SlotLifecycleState expectedState, Assignment expectedAssignment)
    {
        return createSlotStatusWithExpectedState(this.id,
                this.self,
                this.externalUri,
                this.instanceId,
                this.location,
                this.state,
                this.assignment,
                this.installPath,
                this.resources,
                expectedState,
                expectedAssignment,
                this.statusMessage);
    }

    public SlotStatus changeStatusMessage(String statusMessage)
    {
        return new SlotStatus(
                this.id,
                this.shortId,
                this.self,
                this.externalUri,
                this.instanceId,
                this.location,
                this.shortLocation,
                this.state,
                this.assignment,
                this.installPath,
                this.resources,
                this.expectedState,
                this.expectedAssignment,
                statusMessage,
                this.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id,
                self,
                externalUri,
                instanceId,
                location,
                assignment,
                state,
                version,
                expectedState,
                expectedAssignment,
                statusMessage,
                installPath,
                resources);
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
        final SlotStatus other = (SlotStatus) obj;
        return Objects.equal(this.id, other.id) &&
                Objects.equal(this.self, other.self) &&
                Objects.equal(this.externalUri, other.externalUri) &&
                Objects.equal(this.instanceId, other.instanceId) &&
                Objects.equal(this.location, other.location) &&
                Objects.equal(this.assignment, other.assignment) &&
                Objects.equal(this.state, other.state) &&
                Objects.equal(this.version, other.version) &&
                Objects.equal(this.expectedState, other.expectedState) &&
                Objects.equal(this.expectedAssignment, other.expectedAssignment) &&
                Objects.equal(this.statusMessage, other.statusMessage) &&
                Objects.equal(this.installPath, other.installPath) &&
                Objects.equal(this.resources, other.resources);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("self", self)
                .add("externalUri", externalUri)
                .add("instanceId", instanceId)
                .add("location", location)
                .add("assignment", assignment)
                .add("state", state)
                .add("version", version)
                .add("expectedState", expectedState)
                .add("expectedAssignment", expectedAssignment)
                .add("statusMessage", statusMessage)
                .add("installPath", installPath)
                .add("resources", resources)
                .toString();
    }

    public static String createSlotVersion(UUID id, SlotLifecycleState state, Assignment assignment)
    {
        String data = Joiner.on("||").useForNull("--NULL--").join(id, state, assignment);
        return DigestUtils.md5Hex(data);
    }

    public static Function<SlotStatus, String> idGetter()
    {
        return new Function<SlotStatus, String>()
        {
            public String apply(SlotStatus input)
            {
                return input.getId().toString();
            }
        };
    }

    public static Function<SlotStatus, UUID> uuidGetter()
    {
        return new Function<SlotStatus, UUID>()
        {
            public UUID apply(SlotStatus input)
            {
                return input.getId();
            }
        };
    }

    public static Function<SlotStatus, String> locationGetter()
    {
        return new Function<SlotStatus, String>()
        {
            public String apply(SlotStatus input)
            {
                return input.getLocation();
            }
        };
    }

    public static Function<SlotStatus, IdAndVersion> slotToIdWithVersion()
    {
        return new Function<SlotStatus, IdAndVersion>()
        {
            @Override
            public IdAndVersion apply(SlotStatus input)
            {
                return new IdAndVersion(input.getId().toString(), input.getVersion());
            }
        };
    }

    public static Function<SlotStatus, IdAndVersion> slotToIdWithoutVersion()
    {
        return new Function<SlotStatus, IdAndVersion>()
        {
            @Override
            public IdAndVersion apply(SlotStatus input)
            {
                return new IdAndVersion(input.getId().toString(), null);
            }
        };
    }

    public static class SlotStatusFactory
    {
        public static final int MIN_PREFIX_SIZE = 4;
        public static final int MIN_LOCATION_SEGMENTS = 2;

        private final int shortIdPrefixSize;
        private final int commonLocationParts;
        private final Repository repository;

        public SlotStatusFactory(List<SlotStatus> slotStatuses, Repository repository)
        {
            this.shortIdPrefixSize = shortestUniquePrefix(transform(slotStatuses, idGetter()), MIN_PREFIX_SIZE);
            this.commonLocationParts = commonPrefixSegments('/', transform(slotStatuses, locationGetter()), MIN_LOCATION_SEGMENTS);
            this.repository = repository;
        }

        public SlotStatusFactory(int shortIdPrefixSize, int commonLocationParts, Repository repository)
        {
            this.shortIdPrefixSize = shortIdPrefixSize;
            this.commonLocationParts = commonLocationParts;
            this.repository = repository;
        }

        public SlotStatus create(SlotStatus status)
        {
            return new SlotStatus(status.getId(),
                    safeTruncate(status.getId().toString(), shortIdPrefixSize),
                    status.getSelf(),
                    status.getExternalUri(),
                    status.getInstanceId(),
                    status.getLocation(),
                    trimLeadingSegments(status.getLocation(), '/', commonLocationParts),
                    status.getState(),
                    shortenAssignment(repository, status.getAssignment()),
                    status.getInstallPath(),
                    status.getResources(),
                    status.getExpectedState(),
                    shortenAssignment(repository, status.getExpectedAssignment()),
                    status.getStatusMessage(),
                    status.getVersion());
        }
    }

    public static Function<SlotStatus, SlotStatus> shortenSlotStatus(final List<SlotStatus> slotStatuses, final Repository repository)
    {
        return new Function<SlotStatus, SlotStatus>()
        {
            public SlotStatus apply(SlotStatus status)
            {
                return new SlotStatusFactory(slotStatuses, repository).create(status);
            }
        };
    }

    public static SlotStatus shortenSlotStatus(SlotStatus slotStatus, int shortIdPrefixSize, int commonLocationParts, Repository repository)
    {
        return new SlotStatusFactory(shortIdPrefixSize, commonLocationParts, repository).create(slotStatus);
    }
}
