package io.airlift.airship.coordinator.job;

import io.airlift.airship.coordinator.Coordinator;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.airship.coordinator.job.JobInfoUtil.makeJobStatusResponse;

@Path("/v1/job")
public class JobResource
{
    private final Coordinator coordinator;

    @Inject
    public JobResource(Coordinator coordinator)
    {
        this.coordinator = coordinator;
    }

    @GET
    @Path("{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobInfo(@PathParam("jobId") JobId jobId,
            @HeaderParam(AirshipHeaders.AIRSHIP_CURRENT_STATE) String version,
            @HeaderParam(AirshipHeaders.AIRSHIP_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        checkNotNull(jobId, "jobId is null");

        if (maxWait != null) {
            coordinator.waitForJobVersionChange(jobId, version, maxWait);
        }
        JobStatus jobStatus = coordinator.getJobStatus(jobId);
        if (jobStatus == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return makeJobStatusResponse(Response.Status.OK, uriInfo, jobStatus);
    }

    @DELETE
    @Path("{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelJob(@PathParam("jobId") JobId jobId,
            @HeaderParam(AirshipHeaders.AIRSHIP_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        checkNotNull(jobId, "jobId is null");

        if (maxWait == null) {
            maxWait = new Duration(1, TimeUnit.SECONDS);
        }

        JobStatus jobStatus = coordinator.cancelJob(jobId);
        if (jobStatus == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return makeJobStatusResponse(Response.Status.OK, uriInfo, jobStatus);
    }
}
