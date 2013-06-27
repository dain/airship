package io.airlift.airship.coordinator.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

public class JobId
{
    private final String id;

    @JsonCreator
    public JobId(String id)
    {
        Preconditions.checkNotNull(id, "id is null");

        this.id = id;
    }

    @JsonValue
    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobId jobId = (JobId) o;

        if (!id.equals(jobId.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }
}
