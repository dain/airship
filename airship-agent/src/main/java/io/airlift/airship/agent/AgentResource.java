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
import com.google.inject.Inject;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.AirshipHeaders;
import io.airlift.units.Duration;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/agent/")
public class AgentResource
{
    private final Agent agent;

    @Inject
    public AgentResource(Agent agent)
    {
        Preconditions.checkNotNull(agent, "agent is null");

        this.agent = agent;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAgentStatus(@HeaderParam(AirshipHeaders.AIRSHIP_CURRENT_STATE) String currentVersion, @HeaderParam(AirshipHeaders.AIRSHIP_MAX_WAIT) Duration maxWait)
            throws InterruptedException
    {
        if (maxWait != null) {
            try {
                agent.waitForVersionChange(currentVersion, maxWait);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        AgentStatus agentStatus = agent.getAgentStatus();
        return Response.ok(agentStatus).build();
    }
}
