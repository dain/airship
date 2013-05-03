package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;

public class KillTask
        implements Task
{
    @JsonCreator
    public KillTask()
    {
    }

    @Override
    public String getName()
    {
        return "kill";
    }

    @Override
    public String toString()
    {
        return getName();
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return getClass().isInstance(obj);
    }
}
