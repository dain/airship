package io.airlift.airship.coordinator.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.IdAndVersion;

import java.util.List;

public class InstallationRequest
{
    private final Assignment assignment;
    private final List<IdAndVersion> idsAndVersions;

    @JsonCreator
    public InstallationRequest(@JsonProperty("assignment") Assignment assignment, @JsonProperty("idsAndVersions") List<IdAndVersion> idsAndVersions)
    {
        Preconditions.checkNotNull(assignment, "versions is null");
        Preconditions.checkNotNull(idsAndVersions, "idsAndVersions is null");

        this.idsAndVersions = idsAndVersions;
        this.assignment = assignment;
    }

    @JsonProperty
    public Assignment getAssignment()
    {
        return assignment;
    }

    @JsonProperty
    public List<IdAndVersion> getIdsAndVersions()
    {
        return idsAndVersions;
    }
}
