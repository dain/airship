package io.airlift.airship.coordinator.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import io.airlift.airship.shared.DigestUtils;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JobStatus
{
    public static enum JobState
    {
        RUNNING(false),
        DONE(true),
        CANCELED(true),
        FAILED(true);

        private final boolean done;

        private JobState(boolean done)
        {
            this.done = done;
        }

        public boolean isDone()
        {
            return done;
        }
    }

    private final JobId jobId;
    private final URI self;
    private final JobState state;
    private final List<SlotJobStatus> slotJobStatuses;
    private final String version;

    public JobStatus(JobId jobId, URI self, JobState state, List<SlotJobStatus> slotJobStatuses)
    {
        this(jobId, self, state, slotJobStatuses, createJobVersion(jobId, slotJobStatuses));
    }

    @JsonCreator
    public JobStatus(
            @JsonProperty("jobId") JobId jobId,
            @JsonProperty("self") URI self,
            @JsonProperty("state") JobState state,
            @JsonProperty("slots") List<SlotJobStatus> slotJobStatuses,
            @JsonProperty("version") String version)
    {
        this.jobId = jobId;
        this.self = self;
        this.state = state;
        this.slotJobStatuses = slotJobStatuses;
        this.version = version;
    }

    @JsonProperty
    public JobId getJobId()
    {
        return jobId;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public JobState getState()
    {
        return state;
    }

    @JsonProperty("slots")
    public List<SlotJobStatus> getSlotJobStatuses()
    {
        return slotJobStatuses;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(jobId, self, state, slotJobStatuses, version);
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
        final JobStatus other = (JobStatus) obj;
        return Objects.equal(this.jobId, other.jobId) &&
                Objects.equal(this.self, other.self) &&
                Objects.equal(this.state, other.state) &&
                Objects.equal(this.slotJobStatuses, other.slotJobStatuses) &&
                Objects.equal(this.version, other.version);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", jobId)
                .add("self", self)
                .add("state", state)
                .add("slotJobStatuses", slotJobStatuses)
                .add("version", version)
                .toString();
    }

    private static String createJobVersion(JobId jobId, List<SlotJobStatus> slotJobStatuses)
    {
        List<Object> parts = new ArrayList<>();
        parts.add(jobId);

        // canonicalize slot order
        Map<String, SlotJobState> slotVersions = new TreeMap<>();
        for (SlotJobStatus slotJobStatus : slotJobStatuses) {
            slotVersions.put(slotJobStatus.getSlotJobId().toString(), slotJobStatus.getState());
        }
        parts.addAll(slotVersions.values());

        String data = Joiner.on("||").useForNull("--NULL--").join(parts);
        return DigestUtils.md5Hex(data);
    }
}
