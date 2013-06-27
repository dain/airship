package io.airlift.airship.coordinator;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.shared.IdAndVersion;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.List;

import static io.airlift.airship.coordinator.job.JobInfoUtil.makeJobStatusResponse;

@Path("/v1/slot/expected-state")
public class ExpectedStateResource
{
    private final Coordinator coordinator;

    @Inject
    public ExpectedStateResource(Coordinator coordinator)
    {
        Preconditions.checkNotNull(coordinator, "coordinator must not be null");

        this.coordinator = coordinator;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetState(@Context UriInfo uriInfo, List<IdAndVersion> slots)
    {
        if (slots.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("No slots selected")
                    .build();
        }

        // reset slots expected state
        JobStatus jobStatus = coordinator.resetExpectedState(slots);

        return makeJobStatusResponse(Response.Status.CREATED, uriInfo, jobStatus);
    }
}
