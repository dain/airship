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
package io.airlift.airship.coordinator;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.airlift.airship.coordinator.job.InstallationRequest;
import io.airlift.airship.coordinator.job.JobStatus;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static io.airlift.airship.coordinator.job.JobInfoUtil.makeJobStatusResponse;
import static io.airlift.airship.shared.AirshipHeaders.AIRSHIP_FORCE_HEADER;

@Path("/v1/slot/assignment")
public class CoordinatorAssignmentResource
{
    private final Coordinator coordinator;

    @Inject
    public CoordinatorAssignmentResource(Coordinator coordinator)
    {
        Preconditions.checkNotNull(coordinator, "coordinator must not be null");
        this.coordinator = coordinator;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response upgrade(InstallationRequest request,
            @Context UriInfo uriInfo,
            @HeaderParam(AIRSHIP_FORCE_HEADER) boolean force)
    {
        Preconditions.checkNotNull(request, "request must not be null");

        if (request.getIdsAndVersions().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("No slots selected")
                    .build();
        }

        JobStatus jobStatus = coordinator.upgrade(request.getAssignment(), request.getIdsAndVersions(), force);

        return makeJobStatusResponse(Response.Status.CREATED, uriInfo, jobStatus);
    }
}
