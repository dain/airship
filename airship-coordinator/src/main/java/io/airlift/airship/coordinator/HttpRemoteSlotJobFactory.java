package io.airlift.airship.coordinator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.json.JsonCodec;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class HttpRemoteSlotJobFactory
{
    private final AsyncHttpClient httpClient;
    private final Executor executor;
    private final JsonCodec<SlotJob> slotJobCodec;
    private final JsonCodec<SlotJobStatus> slotJobStatusCodec;

    @Inject
    public HttpRemoteSlotJobFactory(@Global AsyncHttpClient httpClient,
            JsonCodec<SlotJob> slotJobCodec,
            JsonCodec<SlotJobStatus> slotJobStatusCodec)
    {
        this.httpClient = httpClient;
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("slot-job-%d").build());
        this.slotJobCodec = slotJobCodec;
        this.slotJobStatusCodec = slotJobStatusCodec;
    }

    public HttpRemoteSlotJob createHttpRemoteSlotJob(URI agentUri, SlotJob slotJob)
    {
        return HttpRemoteSlotJob.createHttpRemoteSlotJob(agentUri, slotJob, httpClient, executor, slotJobCodec, slotJobStatusCodec);
    }
}
