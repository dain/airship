package io.airlift.airship.coordinator.job;

import io.airlift.airship.coordinator.Coordinator;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

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
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStatus(@PathParam("id") String id, @Context UriInfo uriInfo)
    {
        JobStatus jobStatus = coordinator.getJobStatus(new JobId(id));

        return makeJobStatusResponse(Response.Status.OK, uriInfo, jobStatus);
    }
}
