package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.airlift.airship.shared.Installation;

public class InstallTask
    implements Task
{
    private final Installation installation;

    @JsonCreator
    public InstallTask(@JsonProperty("installation") Installation installation)
    {
        this.installation = installation;
    }

    @Override
    public String getName()
    {
        return "install";
    }

    @JsonProperty
    public Installation getInstallation()
    {
        return installation;
    }

    @Override
    public String toString()
    {
        return "install:" + installation.getAssignment().getBinary() + ":" + installation.getAssignment().getConfig();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(installation);
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
        final InstallTask other = (InstallTask) obj;
        return Objects.equal(this.installation, other.installation);
    }
}
