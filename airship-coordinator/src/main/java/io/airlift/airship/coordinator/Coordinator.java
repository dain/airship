package io.airlift.airship.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.airship.coordinator.job.JobExecution;
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
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.InstallTask;
import io.airlift.airship.shared.job.KillTask;
import io.airlift.airship.shared.job.RestartTask;
import io.airlift.airship.shared.job.SlotJob;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.StartTask;
import io.airlift.airship.shared.job.StopTask;
import io.airlift.airship.shared.job.Task;
import io.airlift.airship.shared.job.TerminateTask;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static io.airlift.airship.coordinator.job.JobExecution.createJobExecution;
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
    private final ScheduledExecutorService executor;
    private final Duration statusExpiration;
    private final Provisioner provisioner;
    private final RemoteCoordinatorFactory remoteCoordinatorFactory;
    private final RemoteAgentFactory remoteAgentFactory;
    private final ServiceInventory serviceInventory;
    private final StateManager stateManager;

    private final AtomicLong nextJobId = new AtomicLong();
    private final AtomicLong nextSlotJobId = new AtomicLong();

    private final ConcurrentMap<JobId, JobExecution> jobs = new ConcurrentHashMap<>();

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
                        nodeInfo.getInstanceId(),
                        CoordinatorLifecycleState.ONLINE,
                        nodeInfo.getInstanceId(),
                        httpServerInfo.getHttpUri(),
                        httpServerInfo.getHttpExternalUri(),
                        nodeInfo.getLocation(),
                        nodeInfo.getLocation(),
                        null),
                remoteCoordinatorFactory,
                remoteAgentFactory,
                repository,
                provisioner,
                stateManager,
                serviceInventory,
                checkNotNull(config, "config is null").getStatusExpiration()
        );
    }

    public Coordinator(CoordinatorStatus coordinatorStatus,
            RemoteCoordinatorFactory remoteCoordinatorFactory,
            RemoteAgentFactory remoteAgentFactory,
            Repository repository,
            Provisioner provisioner,
            StateManager stateManager,
            ServiceInventory serviceInventory,
            Duration statusExpiration)
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

        executor = Executors.newScheduledThreadPool(10, new ThreadFactoryBuilder().setNameFormat("coordinator-agent-monitor").setDaemon(true).build());

        updateAllCoordinatorsAndWait();
        updateAllAgents();
    }

    @PostConstruct
    public void start()
    {
        executor.scheduleWithFixedDelay(new Runnable()
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

        executor.scheduleWithFixedDelay(new Runnable()
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

    public JobStatus install(List<IdAndVersion> selectedAgents, Assignment assignment)
    {
        final Installation installation = InstallationUtils.toInstallation(repository, assignment);

        Map<String, RemoteAgent> byAgentId = Maps.uniqueIndex(this.agents.values(), agentIdGetter());

        // todo: update expected state

        List<RemoteSlotJob> slotJobs = new ArrayList<>();
        for (IdAndVersion agentId : selectedAgents) {
            RemoteAgent agent = byAgentId.get(agentId.getId());

            SlotJob slotJob = new SlotJob(nextSlotJobId(), null, ImmutableList.of(new InstallTask(installation)));
            if (agent == null) {
                slotJobs.add(new FailedRemoteSlotJob(slotJob));
            }
            else {
                slotJobs.add(agent.createSlotJob(slotJob));
            }
        }

        return createJob(slotJobs);
    }

    private static Function<RemoteAgent, String> agentIdGetter()
    {
        return new Function<RemoteAgent, String>()
        {
            @Override
            public String apply(RemoteAgent input)
            {
                return input.status().getAgentId();
            }
        };
    }

    private JobStatus createJob(List<RemoteSlotJob> slotJobs)
    {
        JobId jobId = nextJobId();

        JobExecution jobExecution = createJobExecution(this, jobId, slotJobs, executor);
        jobs.putIfAbsent(jobId, jobExecution);

        return jobExecution.getStatus();
    }

    public JobStatus terminate(List<IdAndVersion> slots)
    {
        // 1. todo: update expected state

        // 2. create a job for each slot (slot id + tasks)
        List<RemoteSlotJob> slotJobs = new ArrayList<>();
        for (IdAndVersion slot : slots) {
            RemoteAgent agent = getAgentForSlot(UUID.fromString(slot.getId()));

            SlotJob slotJob = new SlotJob(nextSlotJobId(), UUID.fromString(slot.getId()), ImmutableList.of(new TerminateTask()));
            if (agent == null) {
                slotJobs.add(new FailedRemoteSlotJob(slotJob));
            }
            else {
                slotJobs.add(agent.createSlotJob(slotJob));
            }
        }

        return createJob(slotJobs);
    }

    public JobStatus resetExpectedState(List<IdAndVersion> selectedSlots)
    {
        Set<UUID> slotIds = Stream.on(selectedSlots)
                .transform(idGetter())
                .transform(stringToUuidFunction())
                .set();

        List<SlotStatus> slots = getAllSlotsStatus(compose(in(slotIds), SlotStatus.uuidGetter()));
        Map<String, SlotStatus> byId = Maps.uniqueIndex(slots, SlotStatus.idGetter());

        for (IdAndVersion slot : selectedSlots) {
            SlotStatus currentStatus = byId.get(slot.getId());

            if (currentStatus.getState() != SlotLifecycleState.UNKNOWN) {
                stateManager.setExpectedState(new ExpectedSlotStatus(currentStatus.getId(), currentStatus.getState(), currentStatus.getAssignment()));
            }
            else {
                stateManager.deleteExpectedState(currentStatus.getId());
            }
        }

        return createJob(ImmutableList.<RemoteSlotJob>of()); // todo
    }

    public List<SlotStatus> getAllSlotsStatus()
    {
        ImmutableMap<UUID, ExpectedSlotStatus> expectedStates = Maps.uniqueIndex(stateManager.getAllExpectedStates(), ExpectedSlotStatus.uuidGetter());
        ImmutableMap<UUID, SlotStatus> actualStates = Maps.uniqueIndex(transform(getAllSlots(), getSlotStatus()), SlotStatus.uuidGetter());

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
            stats.add(fullSlotStatus);
        }

        return stats;
    }

    public List<SlotStatus> getAllSlotsStatus(Predicate<SlotStatus> slotFilter)
    {
        return ImmutableList.copyOf(Iterables.filter(getAllSlotsStatus(), slotFilter));
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

            SlotJob slotJob = new SlotJob(nextSlotJobId(), UUID.fromString(slot.getId()), plan(action));
            if (agent == null) {
                slotJobs.add(new FailedRemoteSlotJob(slotJob));
            }
            else {
                slotJobs.add(agent.createSlotJob(slotJob));
            }
        }

        return createJob(slotJobs);
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
    
    public JobStatus upgrade(Assignment assignment, List<IdAndVersion> selectedSlots, boolean force)
    {
        Set<UUID> slotIds = Stream.on(selectedSlots)
                .transform(idGetter())
                .transform(stringToUuidFunction())
                .set();

        List<SlotStatus> slots = getAllSlotsStatus(compose(in(slotIds), SlotStatus.uuidGetter()));
        Map<UUID, SlotStatus> byId = Maps.uniqueIndex(slots, SlotStatus.uuidGetter());

        // 1. todo record expected state

        // 2. create a job for each slot (slot id + tasks)
        List<RemoteSlotJob> slotJobs = new ArrayList<>();
        for (IdAndVersion slot : selectedSlots) {

            Assignment newAssignment;
            SlotStatus status = byId.get(UUID.fromString(slot.getId()));
            if (force && (status.getAssignment() == null)) {
                // allow forced upgrading if existing assignment is missing
                newAssignment = assignment.forceAssignment(repository);
            }
            else {
                newAssignment = assignment.upgradeAssignment(repository, status.getAssignment());
            }

            SlotJob slotJob = new SlotJob(nextSlotJobId(), UUID.fromString(slot.getId()), plan(status, newAssignment));
            RemoteAgent agent = getAgentForSlot(UUID.fromString(slot.getId()));
            if (agent == null) {
                slotJobs.add(new FailedRemoteSlotJob(slotJob));
            }
            else {
                slotJobs.add(agent.createSlotJob(slotJob));
            }
        }

        return createJob(slotJobs);
    }

    public JobStatus getJobStatus(JobId jobId)
    {
        JobExecution job = jobs.get(jobId);
        if (job == null) {
            return null;
        }
        return job.getStatus();
    }

    public void addStateChangeListener(JobId jobId, StateChangeListener<JobStatus> stateChangeListener)
    {
        jobs.get(jobId).addStateChangeListener(stateChangeListener);
    }

    public Duration waitForJobVersionChange(JobId jobId, String currentVersion, Duration maxWait)
            throws InterruptedException
    {
        JobExecution job = jobs.get(jobId);
        if (job == null) {
            return maxWait;
        }
        return job.waitForJobVersionChange(currentVersion, maxWait);
    }

    public JobStatus cancelJob(JobId jobId)
    {
        checkNotNull(jobId, "jobId is null");

        JobExecution job = jobs.get(jobId);
        if (job == null) {
            return null;
        }
        job.cancel();
        return job.getStatus();
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


        URI configFile = repository.configToHttpUri(assignment.getConfig());

        Installation installation = new Installation(
                assignment,
                repository.binaryToHttpUri(assignment.getBinary()),
                configFile, ImmutableMap.<String, Integer>of());

        plan.add(new InstallTask(installation));

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
}
