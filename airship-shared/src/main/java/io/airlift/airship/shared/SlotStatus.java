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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static io.airlift.airship.shared.SlotLifecycleState.UNKNOWN;

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
        return new SlotStatus(id, self, externalUri, instanceId, location, state, assignment, installPath, resources, null, null, null);
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
        return new SlotStatus(id, self, externalUri, instanceId, location, state, assignment, installPath, resources, expectedState, expectedAssignment, statusMessage);
    }

    private final UUID id;
    private final URI self;
    private final URI externalUri;
    private final String instanceId;
    private final String location;
    private final Assignment assignment;
    private final SlotLifecycleState state;
    private final String version;

    private final SlotLifecycleState expectedState;
    private final Assignment expectedAssignment;

    private final String statusMessage;

    private final String installPath;

    private final Map<String, Integer> resources;

    private SlotStatus(UUID id,
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
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkArgument(location.startsWith("/"), "location must start with a /");
        Preconditions.checkNotNull(state, "state is null");
        if (state != TERMINATED && state != UNKNOWN) {
            Preconditions.checkNotNull(assignment, "assignment is null");
        }
        Preconditions.checkNotNull(resources, "resources is null");

        this.id = id;
        this.self = self;
        this.externalUri = externalUri;
        this.instanceId = instanceId;
        this.location = location;
        this.assignment = assignment;
        this.state = state;
        this.version = VersionsUtil.createSlotVersion(id, state, assignment);
        this.installPath = installPath;
        this.expectedState = expectedState;
        this.expectedAssignment = expectedAssignment;
        this.statusMessage = statusMessage;
        this.resources = ImmutableMap.copyOf(resources);
    }


    public UUID getId()
    {
        return id;
    }

    public URI getSelf()
    {
        return self;
    }

    public URI getExternalUri()
    {
        return externalUri;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public String getLocation()
    {
        return location;
    }

    public Assignment getAssignment()
    {
        return assignment;
    }

    public SlotLifecycleState getState()
    {
        return state;
    }

    public String getVersion()
    {
        return version;
    }

    public SlotLifecycleState getExpectedState()
    {
        return expectedState;
    }

    public Assignment getExpectedAssignment()
    {
        return expectedAssignment;
    }

    public String getStatusMessage()
    {
        return statusMessage;
    }

    public String getInstallPath()
    {
        return installPath;
    }

    public Map<String, Integer> getResources()
    {
        return resources;
    }

    public SlotStatus changeState(SlotLifecycleState state)
    {
        return createSlotStatusWithExpectedState(this.id,
                this.self,
                this.externalUri,
                this.instanceId,
                this.location,
                state,
                state == TERMINATED ? null : this.assignment,
                state == TERMINATED ? null : this.installPath,
                state == TERMINATED ? ImmutableMap.<String, Integer>of() : this.resources,
                this.expectedState,
                this.expectedAssignment,
                this.statusMessage);
    }

    public SlotStatus changeInstanceId(String instanceId)
    {
        return createSlotStatusWithExpectedState(this.id,
                this.self,
                this.externalUri,
                instanceId,
                this.location,
                state,
                state == TERMINATED ? null : this.assignment,
                state == TERMINATED ? null : this.installPath,
                state == TERMINATED ? ImmutableMap.<String, Integer>of() : this.resources,
                this.expectedState,
                this.expectedAssignment,
                this.statusMessage);
    }

    public SlotStatus changeAssignment(SlotLifecycleState state, Assignment assignment, Map<String, Integer> resources)
    {
        return createSlotStatusWithExpectedState(this.id,
                this.self,
                this.externalUri,
                this.instanceId,
                this.location,
                state,
                state == TERMINATED ? null : assignment,
                state == TERMINATED ? null : this.installPath,
                state == TERMINATED ? ImmutableMap.<String, Integer>of() : ImmutableMap.copyOf(resources),
                this.expectedState,
                this.expectedAssignment,
                this.statusMessage);
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
        return createSlotStatusWithExpectedState(this.id,
                this.self,
                this.externalUri,
                this.instanceId,
                this.location,
                this.state,
                this.assignment,
                this.installPath,
                this.resources,
                this.expectedState,
                this.expectedAssignment,
                statusMessage);
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
}
