package io.airlift.airship.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.airship.coordinator.job.Job;
import io.airlift.airship.coordinator.job.JobId;
import io.airlift.airship.coordinator.job.JobStatus;
import io.airlift.airship.coordinator.job.SlotLifecycleAction;
import io.airlift.airship.coordinator.job.Stream;
import io.airlift.airship.shared.AgentLifecycleState;
import io.airlift.airship.shared.AgentStatus;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.CoordinatorLifecycleState;
import io.airlift.airship.shared.CoordinatorStatus;
import io.airlift.airship.shared.ExpectedSlotStatus;
import io.airlift.airship.shared.IdAndVersion;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.InstallationUtils;
import io.airlift.airship.shared.Repository;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.job.InstallTask;
import io.airlift.airship.shared.job.KillTask;
import io.airlift.airship.shared.job.RestartTask;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.StartTask;
import io.airlift.airship.shared.job.StopTask;
import io.airlift.airship.shared.job.Task;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static io.airlift.airship.shared.IdAndVersion.idGetter;
import static io.airlift.airship.shared.SlotLifecycleState.RUNNING;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotLifecycleState.UNKNOWN;

public class Coordinator
{
    private static final Logger log = Logger.get(Coordinator.class);

    private final ConcurrentMap<String, RemoteCoordinator> coordinators = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RemoteAgent> agents = new ConcurrentHashMap<>();

    private final CoordinatorStatus coordinatorStatus;
    private final Repository repository;
    private final ScheduledExecutorService timerService;
    private final Duration statusExpiration;
    private final Provisioner provisioner;
    private final RemoteCoordinatorFactory remoteCoordinatorFactory;
    private final RemoteAgentFactory remoteAgentFactory;
    private final ServiceInventory serviceInventory;
    private final StateManager stateManager;
    private final boolean allowDuplicateInstallationsOnAnAgent;
    private final ExecutorService executor;


    private final AtomicLong nextJobId = new AtomicLong();
    private final AtomicLong nextSlotJobId = new AtomicLong();

    private final ConcurrentMap<JobId, Job> jobs = new ConcurrentHashMap<>();

    @Inject
    public Coordinator(NodeInfo nodeInfo,
            HttpServerInfo httpServerInfo,
            CoordinatorConfig config,
            RemoteCoordinatorFactory remoteCoordinatorFactory,
            RemoteAgentFactory remoteAgentFactory,
            Repository repository,
            Provisioner provisioner,
            StateManager stateManager, ServiceInventory serviceInventory)
    {
        this(
                new CoordinatorStatus(nodeInfo.getInstanceId(),
                        CoordinatorLifecycleState.ONLINE,
                        nodeInfo.getInstanceId(),
                        httpServerInfo.getHttpUri(),
                        httpServerInfo.getHttpExternalUri(),
                        nodeInfo.getLocation(),
                        null),
                remoteCoordinatorFactory,
                remoteAgentFactory,
                repository,
                provisioner,
                stateManager,
                serviceInventory,
                checkNotNull(config, "config is null").getStatusExpiration(),
                config.isAllowDuplicateInstallationsOnAnAgent());
    }

    public Coordinator(CoordinatorStatus coordinatorStatus,
            RemoteCoordinatorFactory remoteCoordinatorFactory,
            RemoteAgentFactory remoteAgentFactory,
            Repository repository,
            Provisioner provisioner,
            StateManager stateManager,
            ServiceInventory serviceInventory,
            Duration statusExpiration,
            boolean allowDuplicateInstallationsOnAnAgent)
    {
        Preconditions.checkNotNull(coordinatorStatus, "coordinatorStatus is null");
        Preconditions.checkNotNull(remoteCoordinatorFactory, "remoteCoordinatorFactory is null");
        Preconditions.checkNotNull(remoteAgentFactory, "remoteAgentFactory is null");
        Preconditions.checkNotNull(repository, "repository is null");
        Preconditions.checkNotNull(provisioner, "provisioner is null");
        Preconditions.checkNotNull(stateManager, "stateManager is null");
        Preconditions.checkNotNull(serviceInventory, "serviceInventory is null");
        Preconditions.checkNotNull(statusExpiration, "statusExpiration is null");

        this.coordinatorStatus = coordinatorStatus;
        this.remoteCoordinatorFactory = remoteCoordinatorFactory;
        this.remoteAgentFactory = remoteAgentFactory;
        this.repository = repository;
        this.provisioner = provisioner;
        this.stateManager = stateManager;
        this.serviceInventory = serviceInventory;
        this.statusExpiration = statusExpiration;
        this.allowDuplicateInstallationsOnAnAgent = allowDuplicateInstallationsOnAnAgent;

        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("coordinator-task").build());

        timerService = Executors.newScheduledThreadPool(10, new ThreadFactoryBuilder().setNameFormat("coordinator-agent-monitor").setDaemon(true).build());

        updateAllCoordinatorsAndWait();
        updateAllAgents();
    }

    @PostConstruct
    public void start()
    {
        timerService.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    updateAllCoordinators();
                }
                catch (Throwable e) {
                    log.error(e, "Unexpected exception updating coordinators");
                }
            }
        }, 0, (long) statusExpiration.toMillis(), TimeUnit.MILLISECONDS);

        timerService.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    updateAllAgents();
                }
                catch (Throwable e) {
                    log.error(e, "Unexpected exception updating agents");
                }
            }
        }, 0, (long) statusExpiration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public CoordinatorStatus status()
    {
        return coordinatorStatus;
    }

    public CoordinatorStatus getCoordinator(String instanceId)
    {
        if (coordinatorStatus.getInstanceId().equals(instanceId)) {
            return status();
        }
        RemoteCoordinator remoteCoordinator = coordinators.get(instanceId);
        if (remoteCoordinator == null) {
            return null;
        }
        return remoteCoordinator.status();
    }

    public List<CoordinatorStatus> getCoordinators()
    {
        List<CoordinatorStatus> statuses = ImmutableList.copyOf(Iterables.transform(coordinators.values(), new Function<RemoteCoordinator, CoordinatorStatus>()
        {
            public CoordinatorStatus apply(RemoteCoordinator agent)
            {
                return agent.status();
            }
        }));

        return ImmutableList.<CoordinatorStatus>builder()
                .add(coordinatorStatus)
                .addAll(statuses)
                .build();
    }

    public List<CoordinatorStatus> getCoordinators(Predicate<CoordinatorStatus> coordinatorFilter)
    {
        return ImmutableList.copyOf(filter(getCoordinators(), coordinatorFilter));
    }

    public List<CoordinatorStatus> provisionCoordinators(String coordinatorConfigSpec,
            int coordinatorCount,
            String instanceType,
            String availabilityZone,
            String ami,
            String keyPair,
            String securityGroup)
    {
        List<Instance> instances = provisioner.provisionCoordinators(coordinatorConfigSpec,
                coordinatorCount,
                instanceType,
                availabilityZone,
                ami,
                keyPair,
                securityGroup);

        List<CoordinatorStatus> coordinators = newArrayList();
        for (Instance instance : instances) {
            String instanceId = instance.getInstanceId();

            if (instanceId.equals(this.coordinatorStatus.getInstanceId())) {
                throw new IllegalStateException("Provisioner created a coordinator with the same is as this coordinator");
            }

            RemoteCoordinator remoteCoordinator = remoteCoordinatorFactory.createRemoteCoordinator(instance, CoordinatorLifecycleState.PROVISIONING);
            this.coordinators.put(instanceId, remoteCoordinator);

            coordinators.add(remoteCoordinator.status());
        }
        return coordinators;
    }

    public List<AgentStatus> getAgents()
    {
        return ImmutableList.copyOf(Iterables.transform(agents.values(), new Function<RemoteAgent, AgentStatus>()
        {
            public AgentStatus apply(RemoteAgent agent)
            {
                return agent.status();
            }
        }));
    }

    public List<AgentStatus> getAgents(Predicate<AgentStatus> agentFilter)
    {
        Iterable<RemoteAgent> remoteAgents = filter(this.agents.values(), filterAgentsBy(agentFilter));
        List<AgentStatus> agentStatuses = ImmutableList.copyOf(transform(remoteAgents, getAgentStatus()));
        return agentStatuses;
    }

    public AgentStatus getAgent(String instanceId)
    {
        RemoteAgent remoteAgent = agents.get(instanceId);
        if (remoteAgent == null) {
            return null;
        }
        return remoteAgent.status();
    }

    public AgentStatus getAgentByAgentId(String agentId)
    {
        for (RemoteAgent remoteAgent : agents.values()) {
            AgentStatus status = remoteAgent.status();
            if (agentId.equals(status.getAgentId())) {
                return status;
            }
        }
        return null;
    }

    @VisibleForTesting
    public RemoteAgent getRemoteAgentByInstanceId(String instanceId)
    {
        checkNotNull(instanceId, "instanceId is null");
        for (RemoteAgent remoteAgent : agents.values()) {
            AgentStatus status = remoteAgent.status();
            if (instanceId.equals(status.getInstanceId())) {
                return remoteAgent;
            }
        }
        return null;
    }

    @VisibleForTesting
    public RemoteAgent getRemoteAgentByAgentId(String agentId)
    {
        checkNotNull(agentId, "agentId is null");
        for (RemoteAgent remoteAgent : agents.values()) {
            AgentStatus status = remoteAgent.status();
            if (agentId.equals(status.getAgentId())) {
                return remoteAgent;
            }
        }
        return null;
    }

    @VisibleForTesting
    public void updateAllCoordinatorsAndWait()
    {
        waitForFutures(updateAllCoordinators());
    }

    private List<ListenableFuture<?>> updateAllCoordinators()
    {
        Set<String> instanceIds = newHashSet();
        for (Instance instance : this.provisioner.listCoordinators()) {
            instanceIds.add(instance.getInstanceId());

            // skip this server since it is automatically managed
            if (instance.getInstanceId().equals(this.coordinatorStatus.getInstanceId())) {
                continue;
            }

            RemoteCoordinator remoteCoordinator = remoteCoordinatorFactory.createRemoteCoordinator(instance,
                    instance.getInternalUri() != null ? CoordinatorLifecycleState.ONLINE : CoordinatorLifecycleState.OFFLINE);
            RemoteCoordinator existing = coordinators.putIfAbsent(instance.getInstanceId(), remoteCoordinator);
            if (existing != null) {
                existing.setInternalUri(instance.getInternalUri());
            }
        }

        // add provisioning coordinators to provisioner list
        for (RemoteCoordinator remoteCoordinator : coordinators.values()) {
            if (remoteCoordinator.status().getState() == CoordinatorLifecycleState.PROVISIONING) {
                instanceIds.add(coordinatorStatus.getCoordinatorId());
            }
        }

        // remove any coordinators in the provisioner list
        coordinators.keySet().retainAll(instanceIds);

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (RemoteCoordinator remoteCoordinator : coordinators.values()) {
            futures.add(remoteCoordinator.updateStatus());
        }
        return futures;
    }

    @VisibleForTesting
    public void updateAllAgents()
    {
        Set<String> instanceIds = newHashSet();
        for (Instance instance : this.provisioner.listAgents()) {
            instanceIds.add(instance.getInstanceId());
            RemoteAgent remoteAgent = remoteAgentFactory.createRemoteAgent(instance, AgentLifecycleState.OFFLINE);
            RemoteAgent existing = agents.putIfAbsent(instance.getInstanceId(), remoteAgent);
            if (existing != null) {
                existing.setInternalUri(instance.getInternalUri());
            } else {
                remoteAgent.start();
            }
        }

        // add provisioning agents to provisioner list
        for (RemoteAgent remoteAgent : agents.values()) {
            if (remoteAgent.status().getState() == AgentLifecycleState.PROVISIONING) {
                instanceIds.add(remoteAgent.status().getAgentId());
            }
        }

        // remove any agents not in the provisioner list
        for (String instancesToRemove : Sets.difference(agents.keySet(), instanceIds)) {
            RemoteAgent remoteAgent = agents.remove(instancesToRemove);
            if (remoteAgent != null) {
                remoteAgent.stop();
            }
        }

        List<ServiceDescriptor> serviceDescriptors = serviceInventory.getServiceInventory(transform(getAllSlots(), getSlotStatus()));
        for (RemoteAgent remoteAgent : agents.values()) {
            remoteAgent.setServiceInventory(serviceDescriptors);
        }
    }

    public List<AgentStatus> provisionAgents(String agentConfigSpec,
            int agentCount,
            String instanceType,
            String availabilityZone,
            String ami,
            String keyPair,
            String securityGroup)
    {
        List<Instance> instances = provisioner.provisionAgents(agentConfigSpec,
                agentCount,
                instanceType,
                availabilityZone,
                ami,
                keyPair,
                securityGroup);

        List<AgentStatus> agents = newArrayList();
        for (Instance instance : instances) {
            String instanceId = instance.getInstanceId();

            RemoteAgent remoteAgent = remoteAgentFactory.createRemoteAgent(instance, AgentLifecycleState.PROVISIONING);
            this.agents.put(instanceId, remoteAgent);
            remoteAgent.start();

            agents.add(remoteAgent.status());
        }
        return agents;
    }

    public AgentStatus terminateAgent(String agentId)
    {
        RemoteAgent agent = null;
        for (Iterator<Map.Entry<String, RemoteAgent>> iterator = agents.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, RemoteAgent> entry = iterator.next();
            if (entry.getValue().status().getAgentId().equals(agentId)) {
                iterator.remove();
                agent = entry.getValue();
                break;
            }
        }
        if (agent == null) {
            return null;
        }
        if (!agent.getSlots().isEmpty()) {
            agents.putIfAbsent(agent.status().getInstanceId(), agent);
            throw new IllegalStateException("Cannot terminate agent that has slots: " + agentId);
        }
        agent.stop();
        provisioner.terminateAgents(ImmutableList.of(agentId));
        return agent.status().changeState(AgentLifecycleState.TERMINATED);
    }

    public JobStatus install(List<IdAndVersion> agents, int limit, Assignment assignment)
    {
        throw new UnsupportedOperationException("not yet implemented");
        //        final Installation installation = InstallationUtils.toInstallation(repository, assignment);
        //
        //        List<RemoteAgent> targetAgents = new ArrayList<>(selectAgents(agents, installation));
        //        targetAgents = targetAgents.subList(0, Math.min(targetAgents.size(), limit));
        //
        //        return parallel(targetAgents, new Function<RemoteAgent, SlotStatus>()
        //        {
        //            @Override
        //            public SlotStatus apply(RemoteAgent agent)
        //            {
        //                SlotStatus slotStatus = agent.install(installation);
        //                stateManager.setExpectedState(new ExpectedSlotStatus(slotStatus.getId(), STOPPED, installation.getAssignment()));
        //                return slotStatus;
        //            }
        //        });
    }

    private List<RemoteAgent> selectAgents(Predicate<AgentStatus> filter, Installation installation)
    {
        // select only online agents
        filter = Predicates.and(filter, new AgentFilterBuilder.StatePredicate(AgentLifecycleState.ONLINE));
        List<RemoteAgent> allAgents = newArrayList(filter(this.agents.values(), filterAgentsBy(filter)));
        if (allAgents.isEmpty()) {
            throw new IllegalStateException("No online agents match the provided filters.");
        }
        if (!allowDuplicateInstallationsOnAnAgent) {
            allAgents = newArrayList(filter(allAgents, filterAgentsWithAssignment(installation)));
            if (allAgents.isEmpty()) {
                throw new IllegalStateException("All agents already have the specified binary and configuration installed.");
            }
        }

        // randomize agents so all processes don't end up on the same node
        // todo sort agents by number of process already installed on them?
        Collections.shuffle(allAgents);

        List<RemoteAgent> targetAgents = newArrayList();
        for (RemoteAgent agent : allAgents) {
            // agents without declared resources are considered to have unlimited resources
            AgentStatus status = agent.status();
            if (!status.getResources().isEmpty()) {
                // verify that required resources are available
                Map<String, Integer> availableResources = InstallationUtils.getAvailableResources(status);
                if (!InstallationUtils.resourcesAreAvailable(availableResources, installation.getResources())) {
                    continue;
                }
            }

            targetAgents.add(agent);
        }
        if (targetAgents.isEmpty()) {
            throw new IllegalStateException("No agents have the available resources to run the specified binary and configuration.");
        }
        return targetAgents;
    }

    public JobStatus terminate(List<IdAndVersion> slots)
    {
        throw new UnsupportedOperationException("not yet implemented");
        //        Preconditions.checkNotNull(slots, "slots is null");
        //
        //        // slots the slots
        //        List<RemoteSlot> filteredSlots = selectRemoteSlots(slots);
        //
        //        return parallelCommand(filteredSlots, new Function<RemoteSlot, SlotStatus>()
        //        {
        //            @Override
        //            public SlotStatus apply(RemoteSlot slot)
        //            {
        //                SlotStatus slotStatus = slot.terminate();
        //                if (slotStatus.getState() == TERMINATED) {
        //                    stateManager.deleteExpectedState(slotStatus.getId());
        //                }
        //                return slotStatus;
        //            }
        //        });
    }

    public JobStatus resetExpectedState(List<IdAndVersion> slots)
    {
        throw new UnsupportedOperationException("not yet implemented");
        //
        //        // filter the slots
        //        List<SlotStatus> filteredSlots = getAllSlotsStatus(filter);
        //
        //        return ImmutableList.copyOf(transform(filteredSlots, new Function<SlotStatus, SlotStatus>()
        //        {
        //            @Override
        //            public SlotStatus apply(SlotStatus slotStatus)
        //            {
        //                if (slotStatus.getState() != SlotLifecycleState.UNKNOWN) {
        //                    stateManager.setExpectedState(new ExpectedSlotStatus(slotStatus.getId(), slotStatus.getState(), slotStatus.getAssignment()));
        //                }
        //                else {
        //                    stateManager.deleteExpectedState(slotStatus.getId());
        //                }
        //                return slotStatus;
        //            }
        //        }));
    }

    private List<RemoteSlot> selectRemoteSlots(Predicate<SlotStatus> filter)
    {
        // filter the slots
        List<RemoteSlot> filteredSlots = ImmutableList.copyOf(filter(getAllSlots(), filterSlotsBy(filter)));
        return filteredSlots;
    }

    public List<SlotStatus> getAllSlotStatus()
    {
        return getAllSlotsStatus(Predicates.<SlotStatus>alwaysTrue());
    }

    public List<SlotStatus> getAllSlotsStatus(Predicate<SlotStatus> slotFilter)
    {
        return getAllSlotsStatus(slotFilter, getAllSlots());
    }

    private List<SlotStatus> getAllSlotsStatus(Predicate<SlotStatus> slotFilter, List<RemoteSlot> allSlots)
    {
        ImmutableMap<UUID, ExpectedSlotStatus> expectedStates = Maps.uniqueIndex(stateManager.getAllExpectedStates(), ExpectedSlotStatus.uuidGetter());
        ImmutableMap<UUID, SlotStatus> actualStates = Maps.uniqueIndex(transform(allSlots, getSlotStatus()), SlotStatus.uuidGetter());

        ArrayList<SlotStatus> stats = newArrayList();
        for (UUID uuid : Sets.union(actualStates.keySet(), expectedStates.keySet())) {
            final SlotStatus actualState = actualStates.get(uuid);
            final ExpectedSlotStatus expectedState = expectedStates.get(uuid);

            SlotStatus fullSlotStatus;
            if (actualState == null) {
                // skip terminated slots
                if (expectedState == null || expectedState.getStatus() == SlotLifecycleState.TERMINATED) {
                    continue;
                }
                // missing slot
                fullSlotStatus = SlotStatus.createSlotStatusWithExpectedState(uuid,
                        null,
                        null,
                        null,
                        "/unknown",
                        UNKNOWN,
                        expectedState.getAssignment(),
                        null,
                        ImmutableMap.<String, Integer>of(), expectedState.getStatus(),
                        expectedState.getAssignment(),
                        "Slot is missing; Expected slot to be " + expectedState.getStatus());
            }
            else if (expectedState == null) {
                // unexpected slot
                fullSlotStatus = actualState.changeStatusMessage("Unexpected slot").changeExpectedState(null, null);
            }
            else {
                fullSlotStatus = actualState.changeExpectedState(expectedState.getStatus(), expectedState.getAssignment());

                // add error message if actual state doesn't match expected state
                List<String> messages = newArrayList();
                if (!Objects.equal(actualState.getState(), expectedState.getStatus())) {
                    messages.add("Expected state to be " + expectedState.getStatus());
                }
                if (!Objects.equal(actualState.getAssignment(), expectedState.getAssignment())) {
                    Assignment assignment = expectedState.getAssignment();
                    if (assignment != null) {
                        messages.add("Expected assignment to be " + assignment.getBinary() + " " + assignment.getConfig());
                    }
                    else {
                        messages.add("Expected no assignment");
                    }
                }
                if (!messages.isEmpty()) {
                    fullSlotStatus = fullSlotStatus.changeStatusMessage(Joiner.on("; ").join(messages));
                }
            }
            if (slotFilter.apply(fullSlotStatus)) {
                stats.add(fullSlotStatus);
            }
        }

        return stats;
    }

    public JobStatus doLifecycle(List<IdAndVersion> selectedSlots, SlotLifecycleAction action)
    {
        Set<UUID> slotIds = Stream.on(selectedSlots)
                .transform(idGetter())
                .transform(stringToUuidFunction())
                .set();

        // 1. record new expected state
        // TODO: deal with read-modify-update race condition when updating expected state
        Collection<ExpectedSlotStatus> expectedStates = stateManager.getAllExpectedStates();
        Iterable<ExpectedSlotStatus> toUpdate = Iterables.filter(expectedStates, Predicates.compose(in(slotIds), ExpectedSlotStatus.uuidGetter()));
        for (ExpectedSlotStatus currentExpectedStatus : toUpdate) {
            SlotLifecycleState status;
            switch (action) {
                case STOP:
                case KILL:
                    status = STOPPED;
                    break;
                case START:
                case RESTART:
                    status = RUNNING;
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + action);
            }

            ExpectedSlotStatus updatedStatus = new ExpectedSlotStatus(currentExpectedStatus.getId(), status, currentExpectedStatus.getAssignment());
            stateManager.setExpectedState(updatedStatus);
        }


        // 2. create a job for each slot (slot id + tasks)
        List<RemoteSlotJob> slotJobs = new ArrayList<>();
        for (IdAndVersion slot : selectedSlots) {
            RemoteAgent agent = getAgentForSlot(UUID.fromString(slot.getId()));

            if (agent == null) {
                // TODO: create failed job
            }
            else {
                RemoteSlotJob slotJob = agent.createSlotJob(new SlotJob(nextSlotJobId(), UUID.fromString(slot.getId()), plan(action)));
                slotJobs.add(slotJob);
            }
        }

        JobId jobId = nextJobId();
        Job job = new Job(jobId, slotJobs);

        jobs.putIfAbsent(jobId, job);

        return job.getStatus();
    }


    private RemoteAgent getAgentForSlot(UUID slotId)
    {
        for (RemoteAgent agent : agents.values()) {
            for (RemoteSlot slot : agent.getSlots()) {
                if (slot.getId().equals(slotId)) {
                    return agent;
                }
            }
        }

        return null;
    }
    
    public JobStatus upgrade(Assignment assignments, List<IdAndVersion> selectedSlots, boolean force)
    {
        Set<UUID> slotIds = Stream.on(selectedSlots)
                .transform(idGetter())
                .transform(stringToUuidFunction())
                .set();

        Map<UUID, SlotStatus> actualStatus = Maps.uniqueIndex(getAllSlotStatus(), SlotStatus.uuidGetter());

        List<SlotJob> slotJobs = new ArrayList<>();

        // 1. record new expected state
        // TODO: deal with read-modify-update race condition when updating expected state
        Collection<ExpectedSlotStatus> expectedStates = stateManager.getAllExpectedStates();
        Iterable<ExpectedSlotStatus> toUpdate = Iterables.filter(expectedStates, Predicates.compose(in(slotIds), ExpectedSlotStatus.uuidGetter()));
        for (ExpectedSlotStatus currentExpectedStatus : toUpdate) {
            String binary = assignments.getBinary();
            if (binary == null) {
                binary = currentExpectedStatus.getBinary();
            }

            String config = assignments.getConfig();
            if (config == null) {
                config = currentExpectedStatus.getConfig();
            }

            UUID slotId = currentExpectedStatus.getId();
            Assignment assignment = new Assignment(binary, config);

            ExpectedSlotStatus updatedStatus = new ExpectedSlotStatus(slotId, currentExpectedStatus.getStatus(), assignment);
            stateManager.setExpectedState(updatedStatus);

            // todo: pass version
            SlotJob job = new SlotJob(nextSlotJobId(), slotId, plan(actualStatus.get(slotId), assignment));
            slotJobs.add(job);
        }

        // 3. attach the job to the agent that owns the slot
        // todo

        throw new UnsupportedOperationException("not yet implemented");
    }

    public JobStatus getJobStatus(JobId id)
    {
        return jobs.get(id).getStatus();
    }

    private Predicate<RemoteSlot> filterSlotsBy(final Predicate<SlotStatus> filter)
    {
        return new Predicate<RemoteSlot>()
        {
            @Override
            public boolean apply(RemoteSlot input)
            {
                return filter.apply(input.status());
            }
        };
    }

    private List<RemoteSlot> getAllSlots()
    {
        return ImmutableList.copyOf(concat(Iterables.transform(agents.values(), new Function<RemoteAgent, List<? extends RemoteSlot>>()
        {
            public List<? extends RemoteSlot> apply(RemoteAgent agent)
            {
                return agent.getSlots();
            }
        })));
    }

    private Predicate<RemoteAgent> filterAgentsBy(final Predicate<AgentStatus> filter)
    {
        return new Predicate<RemoteAgent>()
        {
            @Override
            public boolean apply(RemoteAgent input)
            {
                return filter.apply(input.status());
            }
        };
    }

    private Function<RemoteSlot, SlotStatus> getSlotStatus()
    {
        return new Function<RemoteSlot, SlotStatus>()
        {
            @Override
            public SlotStatus apply(RemoteSlot slot)
            {
                return slot.status();
            }
        };
    }

    private Function<RemoteAgent, AgentStatus> getAgentStatus()
    {
        return new Function<RemoteAgent, AgentStatus>()
        {
            @Override
            public AgentStatus apply(RemoteAgent agent)
            {
                return agent.status();
            }
        };
    }

    private Predicate<RemoteAgent> filterAgentsWithAssignment(final Installation installation)
    {
        Preconditions.checkNotNull(installation, "installation is null");
        final Assignment assignment = installation.getAssignment();

        return new Predicate<RemoteAgent>()
        {
            @Override
            public boolean apply(RemoteAgent agent)
            {
                for (RemoteSlot slot : agent.getSlots()) {
                    if (repository.binaryEqualsIgnoreVersion(assignment.getBinary(), slot.status().getAssignment().getBinary()) &&
                            repository.configEqualsIgnoreVersion(assignment.getConfig(), slot.status().getAssignment().getConfig())) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    private <T> ImmutableList<T> parallelCommand(Iterable<RemoteSlot> items, final Function<RemoteSlot, T> function)
    {
        ImmutableCollection<Collection<RemoteSlot>> slotsByInstance = Multimaps.index(items, new Function<RemoteSlot, Object>()
        {
            @Override
            public Object apply(RemoteSlot input)
            {
                return input.status().getInstanceId();
            }
        }).asMap().values();

        // run commands for different instances in parallel
        return ImmutableList.copyOf(concat(parallel(slotsByInstance, new Function<Collection<RemoteSlot>, List<T>>()
        {
            public List<T> apply(Collection<RemoteSlot> input)
            {
                // but run commands for a single instance serially
                return ImmutableList.copyOf(transform(input, function));
            }
        })));
    }

    private <F, T> ImmutableList<T> parallel(Iterable<F> items, final Function<F, T> function)
    {
        List<Callable<T>> callables = ImmutableList.copyOf(transform(items, new Function<F, Callable<T>>()
        {
            public Callable<T> apply(@Nullable final F item)
            {
                return new CallableFunction<>(item, function);
            }
        }));

        List<Future<T>> futures;
        try {
            futures = executor.invokeAll(callables);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for command to finish", e);
        }

        List<Throwable> failures = new ArrayList<>();
        ImmutableList.Builder<T> results = ImmutableList.builder();
        for (Future<T> future : futures) {
            try {
                results.add(future.get());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failures.add(e);
            }
            catch (CancellationException e) {
                failures.add(e);
            }
            catch (ExecutionException e) {
                if (e.getCause() != null) {
                    failures.add(e.getCause());
                } else {
                    failures.add(e);
                }
            }
        }
        if (!failures.isEmpty()) {
            Throwable first = failures.get(0);
            RuntimeException runtimeException = new RuntimeException(first.getMessage());
            for (Throwable failure : failures) {
                runtimeException.addSuppressed(failure);
            }
            throw runtimeException;
        }
        return results.build();
    }

    private static void waitForFutures(Iterable<ListenableFuture<?>> futures)
    {
        try {
            Futures.allAsList(futures).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException ignored) {
        }
    }

    private List<Task> plan(SlotStatus current, Assignment assignment)
    {
        if (current.getAssignment().equals(assignment)) {
            // todo: plan anyway if in force mode?
            return ImmutableList.of();
        }


        List<Task> plan = new ArrayList<>();

        if (current.getState() == RUNNING) {
            plan.add(new StopTask());
        }

        plan.add(new InstallTask(InstallationUtils.toInstallation(repository, assignment)));

        if (current.getState() == RUNNING) {
            plan.add(new StartTask());
        }

        return plan;
    }

    private List<Task> plan(SlotLifecycleAction action)
    {
        Task task;
        switch (action) {
            case STOP:
                task = new StopTask();
                break;
            case START:
                task = new StartTask();
                break;
            case RESTART:
                task = new RestartTask();
                break;
            case KILL:
                task = new KillTask();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported lifecycle action: " + action);
        }

        return ImmutableList.of(task);
    }

    private JobId nextJobId()
    {
        return new JobId(Long.toString(nextJobId.incrementAndGet()));
    }

    private SlotJobId nextSlotJobId()
    {
        return new SlotJobId(Long.toString(nextSlotJobId.incrementAndGet()));
    }

    private Function<String, UUID> stringToUuidFunction()
    {
        return new Function<String, UUID>()
        {
            @Override
            public UUID apply(String input)
            {
                return UUID.fromString(input);
            }
        };
    }



    private static class CallableFunction<F, T>
            implements Callable<T>
    {
        private final F item;
        private final Function<F, T> function;

        private CallableFunction(F item, Function<F, T> function)
        {
            this.item = item;
            this.function = function;
        }

        @Override
        public T call()
        {
            return function.apply(item);
        }

    }

}
