package io.airlift.airship.coordinator.job;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.airlift.airship.coordinator.RemoteSlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;

import javax.annotation.Nullable;
import java.util.List;

public class Job
{
    private final JobId id;
    private final List<RemoteSlotJob> slotJobs;

    public Job(JobId id, List<RemoteSlotJob> slotJobs)
    {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(slotJobs, "slotJobs is null");

        this.id = id;
        this.slotJobs = slotJobs;
    }

    public JobStatus getStatus()
    {
        return new JobStatus(id, Lists.transform(slotJobs, toSlotJobStatus()));
    }


    public static Function<RemoteSlotJob, SlotJobStatus> toSlotJobStatus()
    {
        return new Function<RemoteSlotJob, SlotJobStatus>()
        {
            @Override
            public SlotJobStatus apply(RemoteSlotJob input)
            {
                return input.getJobStatus();
            }
        };
    }
}
