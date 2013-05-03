package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SlotJobId
{
    private final String id;

    @JsonCreator
    public SlotJobId(String id)
    {
        this.id = checkNotNull(id, "id is null");
        checkArgument(!id.isEmpty(), "id is empty");
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id);
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
        final SlotJobId other = (SlotJobId) obj;
        return Objects.equal(this.id, other.id);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return id;
    }
}
