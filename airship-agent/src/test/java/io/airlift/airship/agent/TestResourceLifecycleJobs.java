package io.airlift.airship.agent;

import io.airlift.airship.agent.job.AgentJobResource;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;

import javax.ws.rs.core.Response;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.airlift.airship.agent.ResourcesUtil.TEST_RESOURCES;
import static org.testng.Assert.assertNotNull;

public class TestResourceLifecycleJobs
        extends AbstractTestLifecycleJobs
{
    private Agent agent;
    private AgentJobResource agentJobResource;

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
        agentJobResource = new AgentJobResource(agent);
    }

    @Override
    public Agent getAgent()
    {
        return agent;
    }

    @Override
    protected SlotJobStatus createJob(SlotJob slotJob)
    {
        return getSlotJobStatus(agentJobResource.createJob(slotJob.getSlotJobId(), slotJob));
    }

    @Override
    protected SlotJobStatus updateSlotJobStatus(SlotJob slotJob, SlotJobState currentState)
            throws InterruptedException
    {
        return getSlotJobStatus(agentJobResource.getJobInfo(slotJob.getSlotJobId(), currentState, new Duration(1, TimeUnit.MINUTES)));
    }

    private SlotJobStatus getSlotJobStatus(Response response)
    {
        SlotJobStatus slotJobStatus = (SlotJobStatus) response.getEntity();
        assertNotNull(slotJobStatus, "slotJobStatus is null");
        return slotJobStatus;
    }


}
