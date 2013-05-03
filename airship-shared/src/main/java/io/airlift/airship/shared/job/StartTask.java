package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;

public class StartTask
        implements Task
{
    @JsonCreator
    public StartTask()
    {
    }

    @Override
    public String getName()
    {
        return "start";
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
