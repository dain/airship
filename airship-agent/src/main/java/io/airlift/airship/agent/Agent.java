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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.airship.agent.job.InstallTaskExecution;
import io.airlift.airship.agent.job.SlotJobExecution;
import io.airlift.airship.agent.job.StartTaskExecution;
import io.airlift.airship.agent.job.TaskExecution;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.InstallTask;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.StartTask;
import io.airlift.airship.shared.job.Task;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.airship.shared.AgentLifecycleState.ONLINE;
import static io.airlift.airship.shared.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static java.lang.String.format;

public class Agent
{
    private final String agentId;
    private final ConcurrentMap<UUID, Slot> slots;
    private final DeploymentManagerFactory deploymentManagerFactory;
    private final LifecycleManager lifecycleManager;
    private final String location;
    private final Map<String, Integer> resources;
    private final Duration maxLockWait;
    private final URI internalUri;
    private final URI externalUri;

    private final ConcurrentMap<SlotJobId, SlotJobExecution> jobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, SlotJobId> lockedSlots = new ConcurrentHashMap<>();
    private final Executor executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("slot-job-%d").build());

    @Inject
    public Agent(AgentConfig config,
            HttpServerInfo httpServerInfo,
            NodeInfo nodeInfo,
            DeploymentManagerFactory deploymentManagerFactory,
            LifecycleManager lifecycleManager)
    {
        this(nodeInfo.getNodeId(),
                nodeInfo.getLocation(),
                config.getSlotsDir(),
                httpServerInfo.getHttpUri(),
                httpServerInfo.getHttpExternalUri(),
                config.getResourcesFile(),
                deploymentManagerFactory,
                lifecycleManager,
                config.getMaxLockWait()
        );
    }

    public Agent(
            String agentId,
            String location,
            String slotsDirName,
            URI internalUri,
            URI externalUri,
            String resourcesFilename,
            DeploymentManagerFactory deploymentManagerFactory,
            LifecycleManager lifecycleManager,
            Duration maxLockWait)
    {
        checkNotNull(agentId, "agentId is null");
        checkNotNull(location, "location is null");
        checkNotNull(slotsDirName, "slotsDirName is null");
        checkNotNull(internalUri, "internalUri is null");
        checkNotNull(externalUri, "externalUri is null");
        checkNotNull(deploymentManagerFactory, "deploymentManagerFactory is null");
        checkNotNull(lifecycleManager, "lifecycleManager is null");
        checkNotNull(maxLockWait, "maxLockWait is null");

        this.agentId = agentId;
        this.internalUri = internalUri;
        this.externalUri = externalUri;
        this.maxLockWait = maxLockWait;
        this.location = location;

        this.deploymentManagerFactory = deploymentManagerFactory;
        this.lifecycleManager = lifecycleManager;

        slots = new ConcurrentHashMap<>();

        File slotsDir = new File(slotsDirName);
        if (!slotsDir.isDirectory()) {
            slotsDir.mkdirs();
            checkArgument(slotsDir.isDirectory(), format("Slots directory %s is not a directory", slotsDir));
        }

        //
        // Load existing slots
        //
        for (DeploymentManager deploymentManager : this.deploymentManagerFactory.loadSlots()) {
            UUID slotId = deploymentManager.getSlotId();
            URI slotInternalUri = uriBuilderFrom(internalUri).appendPath("/v1/agent/slot/").appendPath(slotId.toString()).build();
            URI slotExternalUri = uriBuilderFrom(externalUri).appendPath("/v1/agent/slot/").appendPath(slotId.toString()).build();
            Slot slot = new DeploymentSlot(slotInternalUri, slotExternalUri, deploymentManager, lifecycleManager, maxLockWait, executor);
            slots.put(slotId, slot);
        }

        //
        // Load resources file
        //
        Map<String, Integer> resources = ImmutableMap.of();
        if (resourcesFilename != null) {
            File resourcesFile = new File(resourcesFilename);
            if (resourcesFile.canRead()) {
                ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
                Properties properties = new Properties();
                FileInputStream in = null;
                try {
                    in = new FileInputStream(resourcesFile);
                    properties.load(in);
                    for (Entry<Object, Object> entry : properties.entrySet()) {
                        builder.put((String) entry.getKey(), Integer.valueOf((String) entry.getValue()));
                    }
                }
                catch (IOException ignored) {
                }
                finally {
                    Closeables.closeQuietly(in);
                }
                resources = builder.build();
            }
        }
        this.resources = resources;
    }

    public Map<String, Integer> getResources()
    {
        return resources;
    }

    public String getAgentId()
    {
        return agentId;
    }

    public String getLocation()
    {
        return location;
    }

    public AgentStatus getAgentStatus()
    {
        Builder<SlotStatus> builder = ImmutableList.builder();
        for (Slot slot : slots.values()) {
            SlotStatus slotStatus = slot.status();
            builder.add(slotStatus);
        }
        AgentStatus agentStatus = new AgentStatus(agentId, ONLINE, null, internalUri, externalUri, location, null, builder.build(), resources);
        return agentStatus;
    }

    public Slot getSlot(UUID slotId)
    {
        checkNotNull(slotId, "slotId must not be null");

        Slot slot = slots.get(slotId);
        return slot;
    }

    public SlotStatus install(Installation installation)
    {
        return install(installation, null);
    }

    public SlotStatus install(Installation installation, SlotJobId slotJobId)
    {
        // todo name selection is not thread safe
        // create slot
        DeploymentManager deploymentManager = deploymentManagerFactory.createDeploymentManager(installation);
        UUID slotId = deploymentManager.getSlotId();

        URI slotInternalUri = uriBuilderFrom(internalUri).appendPath("/v1/agent/slot/").appendPath(slotId.toString()).build();
        URI slotExternalUri = uriBuilderFrom(externalUri).appendPath("/v1/agent/slot/").appendPath(slotId.toString()).build();
        Slot slot = new DeploymentSlot(slotInternalUri, slotExternalUri, deploymentManager, lifecycleManager, installation, maxLockWait, executor);

        // lock the slot
        if (slotJobId != null) {
            lockedSlots.put(slotId, slotJobId);
        }
        slots.put(slotId, slot);

        // return last slot status
        return slot.getLastSlotStatus();
    }

    public SlotStatus terminateSlot(UUID slotId)
    {
        checkNotNull(slotId, "slotId must not be null");

        Slot slot = slots.get(slotId);
        if (slot == null) {
            return null;
        }

        SlotStatus status = slot.terminate();
        if (status.getState() == TERMINATED) {
            slots.remove(slotId);
        }
        return status;
    }

    public Collection<Slot> getAllSlots()
    {
        return ImmutableList.copyOf(slots.values());
    }

    public SlotJobStatus getJobStatus(SlotJobId slotJobId)
    {
        SlotJobExecution slotJobExecution = jobs.get(slotJobId);
        if (slotJobExecution == null) {
            return null;
        }
        return slotJobExecution.getStatus();
    }

    public void waitForJobStateChange(SlotJobId slotJobId, SlotJobState currentState, Duration maxWait)
            throws InterruptedException
    {
        SlotJobExecution slotJobExecution = jobs.get(slotJobId);
        if (slotJobExecution == null) {
            return;
        }
        slotJobExecution.waitForStateChange(currentState, maxWait);
    }

    public SlotJobStatus createJob(SlotJob slotJob)
    {
        List<TaskExecution> taskExecutions = createExecutionTasks(slotJob);
        final SlotJobExecution slotJobExecution = new SlotJobExecution(this, slotJob.getSlotJobId(), slotJob.getSlotId(), taskExecutions, executor);

        // unlock the slot when the job completes
        slotJobExecution.addStateChangeListener(new StateChangeListener<SlotJobState>() {
            @Override
            public void stateChanged(SlotJobState newValue)
            {
                if (newValue.isDone()) {
                    UUID slotId = slotJobExecution.getSlotId();
                    if (slotId != null) {
                        lockedSlots.remove(slotId, slotJobExecution.getSlotJobId());
                    }

                }
            }
        });

        SlotJobExecution existingJob = jobs.putIfAbsent(slotJob.getSlotJobId(), slotJobExecution);

        // job is already registered
        if (existingJob != null) {
            return existingJob.getStatus();
        } else {
            // job is new, start it
            executor.execute(slotJobExecution);
            return slotJobExecution.getStatus();
        }
    }

    public SlotJobStatus cancelJob(SlotJobId slotJobId, Duration maxWait)
            throws InterruptedException
    {
        checkNotNull(slotJobId, "slotJobId is null");
        checkNotNull(maxWait, "maxWait is null");

        SlotJobExecution slotJobExecution = jobs.get(slotJobId);
        if (slotJobExecution == null) {
            return null;
        }
        slotJobExecution.cancel();
        slotJobExecution.waitForStateChange(SlotJobState.RUNNING, maxWait);
        return slotJobExecution.getStatus();
    }

    public Slot lockSlot(UUID slotId, SlotJobId slotJobId)
    {
        Slot slot = getSlot(slotId);
        checkArgument(slot != null, "Slot %s does not exist", slotId);

        SlotJobId existingJob = lockedSlots.putIfAbsent(slotId, slotJobId);
        checkState(existingJob == null, "Slot %s is already locked by job %s", slotId, slotJobId);

        if (!jobs.containsKey(slotJobId)) {
            lockedSlots.remove(slotId, slotJobId);
            checkState(false, "Job %s is no longer registered", slotJobId);
        }

        return slot;
    }

    private List<TaskExecution> createExecutionTasks(SlotJob slotJob)
    {
        ImmutableList.Builder<TaskExecution> builder = ImmutableList.builder();
        for (Task task : slotJob.getTasks()) {
            if (task instanceof InstallTask) {
                builder.add(new InstallTaskExecution((InstallTask) task));
            }
            else if (task instanceof StartTask) {
                builder.add(new StartTaskExecution((StartTask) task));
            }
            else {
                throw new IllegalArgumentException("Unsupported task type " + task.getClass().getSimpleName());
            }
        }
        return builder.build();
    }
}

