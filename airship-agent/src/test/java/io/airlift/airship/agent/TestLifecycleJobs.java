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

import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.airlift.airship.agent.ResourcesUtil.TEST_RESOURCES;

public class TestLifecycleJobs
        extends AbstractTestLifecycleJobs
{
    private Agent agent;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));

        File resourcesFile = new File(tempDir, "slots/resources.properties");
        ResourcesUtil.writeResources(TEST_RESOURCES, resourcesFile);

        AgentConfig config = new AgentConfig()
                .setSlotsDir(new File(tempDir, "slots").getAbsolutePath())
                .setResourcesFile(resourcesFile.getAbsolutePath());

        agent = new Agent(
                config,
                new HttpServerInfo(new HttpServerConfig(), new NodeInfo("test")),
                new NodeInfo("test"),
                new MockDeploymentManagerFactory(),
                new MockLifecycleManager());
        agent.start();
    }

    @Override
    public Agent getAgent()
    {
        return agent;
    }

    @Override
    protected SlotJobStatus createJob(SlotJob slotJob)
    {
        return agent.createJob(slotJob);
    }

    @Override
    protected SlotJobStatus updateSlotJobStatus(SlotJob slotJob, SlotJobState currentState)
            throws InterruptedException
    {
        agent.waitForJobStateChange(slotJob.getSlotJobId(), currentState, new Duration(1, TimeUnit.MINUTES));
        return agent.getJobStatus(slotJob.getSlotJobId());
    }
}
