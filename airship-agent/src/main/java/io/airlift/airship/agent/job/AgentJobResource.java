package io.airlift.airship.agent.job;

import io.airlift.airship.agent.Agent;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/v1/agent/job")
public class AgentJobResource
{
    private final Agent agent;

    @Inject
    public AgentJobResource(Agent agent)
    {
        this.agent = agent;
    }

    @POST
    @Path("{slotJobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createJob(@PathParam("slotJobId") SlotJobId slotJobId, SlotJob slotJob)
    {
        checkNotNull(slotJob, "slotJob is null");
        SlotJobStatus slotJobStatus = agent.createJob(slotJob);
        Response response = Response.ok().entity(slotJobStatus).build();
        return response;
    }

    @GET
    @Path("{slotJobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobInfo(@PathParam("slotJobId") SlotJobId slotJobId,
            @HeaderParam(AirshipHeaders.AIRSHIP_CURRENT_STATE) SlotJobState currentState,
            @HeaderParam(AirshipHeaders.AIRSHIP_MAX_WAIT) Duration maxWait)
            throws InterruptedException
    {
        checkNotNull(slotJobId, "slotJobId is null");

        if (maxWait != null) {
            agent.waitForJobStateChange(slotJobId, currentState, maxWait);
        }
        SlotJobStatus jobStatus = agent.getJobStatus(slotJobId);
        if (jobStatus == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.ok(jobStatus).build();
    }

    @DELETE
    @Path("{slotJobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelJob(@PathParam("slotJobId") SlotJobId slotJobId,
            @HeaderParam(AirshipHeaders.AIRSHIP_MAX_WAIT) Duration maxWait)
            throws InterruptedException
    {
        checkNotNull(slotJobId, "slotJobId is null");

        if (maxWait == null) {
            maxWait = new Duration(1, TimeUnit.SECONDS);
        }

        SlotJobStatus jobStatus = agent.cancelJob(slotJobId, maxWait);
        if (jobStatus == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.ok(jobStatus).build();
    }
}
