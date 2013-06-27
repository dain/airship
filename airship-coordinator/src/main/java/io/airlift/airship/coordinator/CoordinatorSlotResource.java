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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.airlift.airship.coordinator.job.InstallationRequest;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.Repository;
import io.airlift.airship.shared.SlotStatus;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.transform;
import static io.airlift.airship.coordinator.job.JobInfoUtil.makeJobStatusResponse;
import static io.airlift.airship.shared.SlotStatus.uuidGetter;
import static io.airlift.airship.shared.SlotStatusRepresentation.fromSlotStatus;

@Path("/v1/slot")
public class CoordinatorSlotResource
{
    public static final int MIN_PREFIX_SIZE = 4;

    private final Coordinator coordinator;
    private final Repository repository;

    @Inject
    public CoordinatorSlotResource(Coordinator coordinator, Repository repository)
    {
        Preconditions.checkNotNull(coordinator, "coordinator must not be null");
        Preconditions.checkNotNull(repository, "repository is null");

        this.coordinator = coordinator;
        this.repository = repository;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllSlots(@Context UriInfo uriInfo)
    {
        // build filter
        List<UUID> uuids = transform(coordinator.getAllSlotStatus(), uuidGetter());
        Predicate<SlotStatus> slotFilter = SlotFilterBuilder.build(uriInfo, false, uuids);

        // select slots
        List<SlotStatus> slots = coordinator.getAllSlotsStatus(slotFilter);

        // build response
        return Response.ok(Iterables.transform(slots, fromSlotStatus(coordinator.getAllSlotStatus(), repository)))
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response install(
            InstallationRequest request,
            @DefaultValue("1") @QueryParam("limit") int limit,
            @Context UriInfo uriInfo)
    {
        Preconditions.checkNotNull(request, "request must not be null");
        Preconditions.checkArgument(limit > 0, "limit must be at least 1");

        if (request.getIdsAndVersions().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("No agents selected")
                    .build();
        }

        // install the software
        JobStatus job = coordinator.install(request.getIdsAndVersions(), limit, request.getAssignment());

        return makeJobStatusResponse(Response.Status.CREATED, uriInfo, job);
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public Response terminateSlots(@Context UriInfo uriInfo, List<IdAndVersion> slots)
    {
        // terminate slots
        JobStatus job = coordinator.terminate(slots);

        return makeJobStatusResponse(Response.Status.CREATED, uriInfo, job);
    }
}
