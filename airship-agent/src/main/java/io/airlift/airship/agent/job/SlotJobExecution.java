package io.airlift.airship.agent.job;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.airship.agent.Agent;
import io.airlift.airship.agent.Progress;
import io.airlift.airship.agent.Slot;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.airship.shared.StateMachine;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.airship.shared.job.SlotJobId;
import io.airlift.airship.shared.job.SlotJobStatus;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.TaskStatus;
import io.airlift.airship.shared.job.TaskStatus.TaskState;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class SlotJobExecution
        implements Runnable
{
    private final Agent agent;
    private final SlotJobId slotJobId;
    private final AtomicReference<UUID> slotId;
    private final List<TaskExecution> tasks;
    private final StateMachine<SlotJobState> state;
    private final Progress progress = new Progress();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final URI self;

    public SlotJobExecution(Agent agent, SlotJobId slotJobId, UUID slotId, List<TaskExecution> tasks, Executor executor)
    {
        this.agent = checkNotNull(agent, "agent is null");
        this.slotJobId = checkNotNull(slotJobId, "jobId is null");
        this.self = uriBuilderFrom(agent.getAgentStatus().getInternalUri()).replacePath("v1/agent/job").appendPath(slotJobId.toString()).build();
        this.slotId = new AtomicReference<>(slotId);
        this.tasks = checkNotNull(tasks, "tasks is null");
        this.state = new StateMachine<>("slotJob " + slotId, executor, SlotJobState.PENDING);
    }

    public SlotJobId getSlotJobId()
    {
        return slotJobId;
    }

    public UUID getSlotId()
    {
        return slotId.get();
    }

    public SlotJobStatus getStatus()
    {
        SlotStatusRepresentation slotStatus = null;
        if (slotId.get() != null) {
            slotStatus = SlotStatusRepresentation.from(agent.getSlot(slotId.get()).status());
        }
        return new SlotJobStatus(slotJobId,
                self,
                state.get(),
                slotStatus,
                progress.getDescription(),
                progress.getProgress(),
                ImmutableList.copyOf(transform(tasks, taskStatusGetter())));
    }

    public void cancel()
    {
        canceled.set(true);
    }

    public void addStateChangeListener(StateChangeListener<SlotJobState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    public Duration waitForStateChange(SlotJobState currentState, Duration maxWait)
            throws InterruptedException
    {
        return state.waitForStateChange(currentState, maxWait);
    }

    public void run()
    {
        checkState(state.compareAndSet(SlotJobState.PENDING, SlotJobState.RUNNING), "SlotJob %s is already running", slotId.get());
        try {
            Slot slot;
            List<TaskExecution> tasks;
            if (slotId.get() == null) {
                TaskExecution task = this.tasks.get(0);
                checkArgument(task instanceof InstallTaskExecution, "First task of a new slot must be an install task");
                Installation installation = ((InstallTaskExecution) task).getInstallation();

                SlotStatus slotStatus = agent.install(installation);
                slotId.set(slotStatus.getId());
                slot = agent.getSlot(slotStatus.getId());
                tasks = this.tasks.subList(1, this.tasks.size());
            }
            else {
                // lock slot; slot will automatically unlock when job is complete
                slot = agent.lockSlot(slotId.get(), slotJobId);
                checkState(slot != null, "Slot %s does not exist");
                tasks = this.tasks;
            }

            for (TaskExecution task : tasks) {
                try {
                    if (!canceled.get() && state.get() == SlotJobState.RUNNING) {
                        task.run(slot, progress);
                    } else {
                        task.skip();
                    }

                    if (task.getStatus().getState() != TaskState.DONE) {
                        state.set(SlotJobState.FAILED);
                    }
                }
                catch (Throwable e) {
                    state.set(SlotJobState.FAILED);
                    Throwables.propagateIfInstanceOf(e, Error.class);
                }
            }
        }
        finally {
            state.compareAndSet(SlotJobState.RUNNING, SlotJobState.DONE);
        }
    }

    private Function<TaskExecution, TaskStatus> taskStatusGetter()
    {
        return new Function<TaskExecution, TaskStatus>()
        {
            @Override
            public TaskStatus apply(TaskExecution taskExecution)
            {
                return taskExecution.getStatus();
            }
        };
    }
}
