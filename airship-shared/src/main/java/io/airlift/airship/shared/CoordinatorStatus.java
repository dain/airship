package io.airlift.airship.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.Immutable;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.collect.Lists.transform;
import static io.airlift.airship.shared.Strings.commonPrefixSegments;
import static io.airlift.airship.shared.Strings.safeTruncate;
import static io.airlift.airship.shared.Strings.shortestUniquePrefix;
import static io.airlift.airship.shared.Strings.trimLeadingSegments;

@Immutable
public class CoordinatorStatus
{
    private final String coordinatorId;
    private final String shortCoordinatorId;
    private final CoordinatorLifecycleState state;
    private final String instanceId;
    private final URI internalUri;
    private final URI externalUri;
    private final String location;
    private final String shortLocation;
    private final String instanceType;
    private final String version;

    public CoordinatorStatus(
             String coordinatorId,
             String shortCoordinatorId,
             CoordinatorLifecycleState state,
             String instanceId,
             URI internalUri,
             URI externalUri,
             String location,
             String shortLocation,
             String instanceType)
    {
        this(coordinatorId,
                shortCoordinatorId,
                state,
                instanceId,
                internalUri,
                externalUri,
                location,
                shortLocation,
                instanceType,
                createVersion(coordinatorId, state));
    }

    @JsonCreator
    public CoordinatorStatus(
            @JsonProperty("coordinatorId") String coordinatorId,
            @JsonProperty("shortCoordinatorId") String shortCoordinatorId,
            @JsonProperty("state") CoordinatorLifecycleState state,
            @JsonProperty("instanceId") String instanceId,
            @JsonProperty("internalUri") URI internalUri,
            @JsonProperty("externalUri") URI externalUri,
            @JsonProperty("location") String location,
            @JsonProperty("shortLocation") String shortLocation,
            @JsonProperty("instanceType") String instanceType,
            @JsonProperty("version") String version)
    {
        Preconditions.checkNotNull(state, "state is null");
        Preconditions.checkNotNull(instanceId, "instanceId is null");
        Preconditions.checkArgument(!instanceId.isEmpty(), "instanceId is empty");

        this.coordinatorId = coordinatorId;
        this.shortCoordinatorId = shortCoordinatorId;
        this.state = state;
        this.instanceId = instanceId;
        this.internalUri = internalUri;
        this.externalUri = externalUri;
        this.location = location;
        this.shortLocation = shortLocation;
        this.instanceType = instanceType;
        this.version = version;
    }

    @JsonProperty
    public String getCoordinatorId()
    {
        return coordinatorId;
    }

    @JsonProperty
    public String getShortCoordinatorId()
    {
        return shortCoordinatorId;
    }

    @JsonProperty
    public CoordinatorLifecycleState getState()
    {
        return state;
    }

    public CoordinatorStatus changeState(CoordinatorLifecycleState state)
    {
        return new CoordinatorStatus(coordinatorId, shortCoordinatorId, state, instanceId, internalUri, externalUri, location, shortLocation, instanceType);
    }

    public CoordinatorStatus changeInternalUri(URI internalUri)
    {
        return new CoordinatorStatus(coordinatorId, shortCoordinatorId, state, instanceId, internalUri, externalUri, location, shortLocation, instanceType);
    }

    public CoordinatorStatus changeInstance(String instanceId, String instanceType)
    {
        return new CoordinatorStatus(coordinatorId, shortCoordinatorId, state, instanceId, internalUri, externalUri, location, shortLocation, instanceType);
    }

    @JsonProperty
    public String getInstanceId()
    {
        return instanceId;
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

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(
                coordinatorId,
                shortCoordinatorId,
                state,
                instanceId,
                internalUri,
                externalUri,
                location,
                shortLocation,
                instanceType,
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
        final CoordinatorStatus other = (CoordinatorStatus) obj;
        return Objects.equal(this.coordinatorId, other.coordinatorId) &&
                Objects.equal(this.shortCoordinatorId, other.shortCoordinatorId) &&
                Objects.equal(this.state, other.state) &&
                Objects.equal(this.instanceId, other.instanceId) &&
                Objects.equal(this.internalUri, other.internalUri) &&
                Objects.equal(this.externalUri, other.externalUri) &&
                Objects.equal(this.location, other.location) &&
                Objects.equal(this.shortLocation, other.shortLocation) &&
                Objects.equal(this.instanceType, other.instanceType) &&
                Objects.equal(this.version, other.version);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("coordinatorId", coordinatorId)
                .add("shortCoordinatorId", shortCoordinatorId)
                .add("state", state)
                .add("instanceId", instanceId)
                .add("internalUri", internalUri)
                .add("externalUri", externalUri)
                .add("location", location)
                .add("shortLocation", shortLocation)
                .add("instanceType", instanceType)
                .add("version", version)
                .toString();
    }

    private static String createVersion(String coordinatorId, CoordinatorLifecycleState state)
    {
        List<Object> parts = new ArrayList<>();
        parts.add(coordinatorId);
        parts.add(state);

        String data = Joiner.on("||").useForNull("--NULL--").join(parts);
        return DigestUtils.md5Hex(data);
    }

    public static Function<CoordinatorStatus, String> idGetter()
    {
        return new Function<CoordinatorStatus, String>()
        {
            public String apply(CoordinatorStatus input)
            {
                return input.getCoordinatorId();
            }
        };
    }

    public static Function<CoordinatorStatus, String> locationGetter(final String defaultValue)
    {
        return new Function<CoordinatorStatus, String>()
        {
            public String apply(CoordinatorStatus input)
            {
                return firstNonNull(input.getLocation(), defaultValue);
            }
        };
    }

    public static Function<CoordinatorStatus, CoordinatorStatus> fromCoordinatorStatus(List<CoordinatorStatus> coordinatorStatuses)
    {
        return fromCoordinatorStatus(new CoordinatorStatusFactory(coordinatorStatuses));
    }

    public static Function<CoordinatorStatus, CoordinatorStatus> fromCoordinatorStatus(final CoordinatorStatusFactory factory)
    {
        return new Function<CoordinatorStatus, CoordinatorStatus>()
        {
            public CoordinatorStatus apply(CoordinatorStatus status)
            {
                return factory.create(status);
            }
        };
    }

    private static class CoordinatorStatusFactory
    {
        public static final int MIN_PREFIX_SIZE = 4;
        public static final int MIN_LOCATION_SEGMENTS = 2;

        private final int shortIdPrefixSize;
        private final int commonLocationParts;

        public CoordinatorStatusFactory(List<CoordinatorStatus> coordinatorStatuses)
        {
            this.shortIdPrefixSize = shortestUniquePrefix(transform(coordinatorStatuses, idGetter()), MIN_PREFIX_SIZE);
            this.commonLocationParts = commonPrefixSegments('/', transform(coordinatorStatuses, locationGetter("/")), MIN_LOCATION_SEGMENTS);
        }

        public CoordinatorStatus create(CoordinatorStatus status)
        {
            return new CoordinatorStatus(
                    status.getCoordinatorId(),
                    safeTruncate(status.getCoordinatorId(), shortIdPrefixSize),
                    status.getState(),
                    status.getInstanceId(),
                    status.getInternalUri(),
                    status.getExternalUri(),
                    status.getLocation(),
                    trimLeadingSegments(status.getLocation(), '/', commonLocationParts),
                    status.getInstanceType(),
                    status.getVersion());
        }
    }
}
