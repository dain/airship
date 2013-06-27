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
package io.airlift.airship.agent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.airship.shared.InstallationRepresentation;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.SlotStatusRepresentation;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;

@Path("/v1/agent/slot")
public class SlotResource
{
    private final Agent agent;

    @Inject
    public SlotResource(Agent agent)
    {
        Preconditions.checkNotNull(agent, "agent is null");

        this.agent = agent;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response installSlot(InstallationRepresentation installation, @Context UriInfo uriInfo)
    {
        Preconditions.checkNotNull(installation, "installation must not be null");

        SlotStatus slotStatus = agent.install(installation.toInstallation());

        return Response
                .created(getSelfUri(slotStatus.getId(), uriInfo.getBaseUri()))
                .entity(SlotStatusRepresentation.from(slotStatus))
                .build();
    }

    @Path("{slotId}")
    @DELETE
    public Response terminateSlot(@PathParam("slotId") UUID slotId)
    {
        Preconditions.checkNotNull(slotId, "slotId must not be null");

        Slot slot = agent.getSlot(slotId);
        if (slot == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        SlotStatus slotStatus;
        try {
            slotStatus = agent.terminateSlot(slotId);
        }
        catch (IllegalStateException e) {
            return Response.status(Status.CONFLICT).build();
        }
        if (slotStatus == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok(SlotStatusRepresentation.from(slotStatus)).build();
    }

    @Path("{slotId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSlotStatus(@PathParam("slotId") UUID slotId, @Context UriInfo uriInfo)
    {
        Preconditions.checkNotNull(slotId, "slotId must not be null");

        Slot slot = agent.getSlot(slotId);
        if (slot == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        SlotStatus slotStatus = slot.status();
        return Response.ok(SlotStatusRepresentation.from(slotStatus)).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllSlotsStatus(@Context UriInfo uriInfo)
    {
        List<SlotStatusRepresentation> representations = Lists.newArrayList();
        for (Slot slot : agent.getAllSlots()) {
            SlotStatus slotStatus = slot.status();
            representations.add(SlotStatusRepresentation.from(slotStatus));
        }
        return Response.ok(representations).build();
    }


    private static URI getSelfUri(UUID slotId, URI baseUri)
    {
        return uriBuilderFrom(baseUri).appendPath("/v1/agent/slot/").appendPath(slotId.toString()).build();
    }
}
