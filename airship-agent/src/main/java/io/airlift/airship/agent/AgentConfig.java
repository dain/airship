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
package io.airlift.airship.agent;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class AgentConfig
{
    private String slotsDir = "slots";
    private String resourcesFile = "etc/resources.properties";
    private Duration launcherTimeout = new Duration(5, TimeUnit.SECONDS);
    private Duration launcherStopTimeout = new Duration(1, TimeUnit.SECONDS);
    private Duration tarTimeout = new Duration(1, TimeUnit.MINUTES);
    private Duration updateInterval = new Duration(5, TimeUnit.SECONDS);
    private Duration maxLockWait = new Duration(1, TimeUnit.SECONDS);

    @NotNull
    public String getSlotsDir()
    {
        return slotsDir;
    }

    @Config("agent.slots-dir")
    public AgentConfig setSlotsDir(String slotsDir)
    {
        this.slotsDir = slotsDir;
        return this;
    }

    @NotNull
    public String getResourcesFile()
    {
        return resourcesFile;
    }

    @Config("agent.resources-file")
    public AgentConfig setResourcesFile(String resourcesFile)
    {
        this.resourcesFile = resourcesFile;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getLauncherTimeout()
    {
        return launcherTimeout;
    }

    @Config("agent.launcher-timeout")
    public AgentConfig setLauncherTimeout(Duration launcherTimeout)
    {
        this.launcherTimeout = launcherTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getLauncherStopTimeout()
    {
        return launcherStopTimeout;
    }

    @Config("agent.launcher-stop-timeout")
    public AgentConfig setLauncherStopTimeout(Duration launcherStopTimeout)
    {
        this.launcherStopTimeout = launcherStopTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getTarTimeout()
    {
        return tarTimeout;
    }

    @Config("agent.tar-timeout")
    public AgentConfig setTarTimeout(Duration tarTimeout)
    {
        this.tarTimeout = tarTimeout;
        return this;
    }

    @NotNull
    public Duration getUpdateInterval()
    {
        return updateInterval;
    }

    @Config("agent.update-interval")
    public AgentConfig setUpdateInterval(Duration updateInterval)
    {
        this.updateInterval = updateInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getMaxLockWait()
    {
        return maxLockWait;
    }

    @Config("agent.max-lock-wait")
    public AgentConfig setMaxLockWait(Duration lockWait)
    {
        this.maxLockWait = lockWait;
        return this;
    }
}
