package io.airlift.airship.coordinator;

import com.google.common.collect.ImmutableList;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.TaskStatus;

import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class MockRemoteSlotJob
        implements RemoteSlotJob
{
    private final URI agentUri;
    private final SlotJob slotJob;
    private final SlotStatusRepresentation slotStatus;

    public MockRemoteSlotJob(URI agentUri, SlotJob slotJob, SlotStatusRepresentation slotStatus)
    {
        this.agentUri = agentUri;
        this.slotStatus = slotStatus;
        this.slotJob = slotJob;
    }

    @Override
    public SlotJobStatus getJobStatus()
    {
        return new SlotJobStatus(slotJob.getSlotJobId(),
                uriBuilderFrom(agentUri).replacePath("v1/agent/job").appendPath(slotJob.getSlotJobId().toString()).build(),
                SlotJobStatus.SlotJobState.DONE,
                slotStatus,
                null,
                0.0,
                ImmutableList.<TaskStatus>of());
    }
}
