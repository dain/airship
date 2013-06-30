package io.airlift.airship.coordinator.job;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

public class JobInfoUtil
{
    public static Response makeJobStatusResponse(Response.Status status, UriInfo uriInfo, JobStatus jobStatus)
    {
        URI nextUri = uriInfo.getBaseUriBuilder()
                .replacePath("/v1/job")
                .path(jobStatus.getJobId().toString())
                .build();

        return Response.status(status)
                .entity(jobStatus)
                .location(nextUri)
                .build();
    }

}
