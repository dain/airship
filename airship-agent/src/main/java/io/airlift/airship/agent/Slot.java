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

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.StateMachine;
import io.airlift.airship.shared.StateMachine.StateChangeListener;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.airship.shared.SlotLifecycleState.RUNNING;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static io.airlift.airship.shared.SlotLifecycleState.TERMINATED;
import static io.airlift.airship.shared.SlotLifecycleState.UNKNOWN;
import static io.airlift.airship.shared.SlotStatus.createSlotStatus;

public class Slot
{
    private static final Logger log = Logger.get(Slot.class);

    private final UUID id;
    private final String location;
    private final URI self;
    private final URI externalUri;
    private final Duration updateInterval;
    private final Duration lockWait;
    private final DeploymentManager deploymentManager;
    private final LifecycleManager lifecycleManager;
    private final StateMachine<SlotStatus> slotStatus;
    private final Executor executor;
    private boolean terminated;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile Thread lockOwner;
    private volatile List<StackTraceElement> lockAcquisitionLocation;

    public static Slot createNewDeploymentSlot(URI self,
            URI externalUri,
            DeploymentManager deploymentManager,
            LifecycleManager lifecycleManager,
            Installation installation,
            Duration updateInterval,
            Duration maxLockWait,
            Executor executor)
    {
        Slot slot = new Slot(self, externalUri, deploymentManager, lifecycleManager, updateInterval, maxLockWait, executor);

        // install the software
        try {
            slot.assign(installation, new Progress());
            slot.startUpdateLoop();
            return slot;
        }
        catch (Exception e) {
            slot.terminate();
            throw Throwables.propagate(e);
        }

    }

    public static Slot loadDeploymentSlot(URI self,
            URI externalUri,
            DeploymentManager deploymentManager,
            LifecycleManager lifecycleManager,
            Duration updateInterval,
            Duration maxLockWait,
            Executor executor)
    {
        Slot slot = new Slot(self, externalUri, deploymentManager, lifecycleManager, updateInterval, maxLockWait, executor);
        slot.startUpdateLoop();
        return slot;
    }

    private Slot(URI self,
            URI externalUri,
            DeploymentManager deploymentManager,
            LifecycleManager lifecycleManager,
            Duration updateInterval,
            Duration maxLockWait,
            Executor executor)
    {
        checkNotNull(self, "self is null");
        checkNotNull(externalUri, "externalUri is null");
        checkNotNull(deploymentManager, "deploymentManager is null");
        checkNotNull(lifecycleManager, "lifecycleManager is null");
        checkNotNull(maxLockWait, "maxLockWait is null");
        checkNotNull(executor, "executor is null");

        this.location = deploymentManager.getLocation();
        this.deploymentManager = deploymentManager;
        this.lifecycleManager = lifecycleManager;

        this.updateInterval = updateInterval;
        this.lockWait = maxLockWait;
        this.id = deploymentManager.getSlotId();
        this.self = self;
        this.externalUri = externalUri;
        this.slotStatus = new StateMachine<>("slot " + id, executor, null);
        this.executor = executor;

        Deployment deployment = deploymentManager.getDeployment();
        if (deployment == null) {
            changeSlotStatus(createSlotStatus(id,
                    self,
                    externalUri,
                    null,
                    location,
                    UNKNOWN,
                    null,
                    deploymentManager.hackGetDataDir().getAbsolutePath(),
                    ImmutableMap.<String, Integer>of()));
        }
        else {
            SlotLifecycleState state = lifecycleManager.status(deployment);
            changeSlotStatus(createSlotStatus(id,
                    self,
                    externalUri,
                    null,
                    location,
                    state,
                    deployment.getAssignment(),
                    deployment.getDataDir().getAbsolutePath(),
                    deployment.getResources()));
        }
    }

    private void startUpdateLoop()
    {
        if (updateInterval.toMillis() >= 1) {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    String originalThreadName = Thread.currentThread().getName();
                    try {
                        Thread.currentThread().setName("slot-update-" + id);

                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                SlotStatus status = updateStatus();
                                if (status.getState() == TERMINATED || Thread.currentThread().isInterrupted()) {
                                    return;
                                }
                            }
                            catch (Throwable e) {
                                log.warn(e, "Error updating status of %s", id);
                            }

                            try {
                                TimeUnit.MILLISECONDS.sleep((long) updateInterval.toMillis());
                            }
                            catch (InterruptedException e) {
                                return;
                            }
                        }
                    }
                    finally {
                        Thread.currentThread().setName(originalThreadName);
                    }
                }
            });
        }
    }

    public UUID getId()
    {
        return id;
    }

    public URI getSelf()
    {
        return self;
    }

    public URI getExternalUri()
    {
        return externalUri;
    }

    public SlotStatus assign(Installation installation, Progress progress)
    {
        checkNotNull(installation, "installation is null");

        lock();
        try {
            checkNotNull(progress, "progress is null");
            checkState(!terminated, "Slot has been terminated");

            progress.reset("Checking status");
            // check current actual status
            SlotStatus status = updateStatus();
            checkState(status.getState() == STOPPED || status.getState() == UNKNOWN, "Slot is not stopped, but is %s", status.getState());

            log.info("Becoming %s with %s", installation.getAssignment().getBinary(), installation.getAssignment().getConfig());

            // deploy new server
            Deployment deployment = deploymentManager.install(installation, progress);

            // create node config file
            lifecycleManager.updateNodeConfig(deployment);

            SlotStatus slotStatus = createSlotStatus(id,
                    self,
                    externalUri,
                    null,
                    location,
                    STOPPED,
                    installation.getAssignment(),
                    deployment.getDataDir().getAbsolutePath(),
                    deployment.getResources());

            changeSlotStatus(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    private synchronized  void changeSlotStatus(SlotStatus newStatus)
    {
        SlotStatus oldStatus = slotStatus.set(newStatus);
//        new Exception(String.format("Slot %s changed from %s to %s(%s)", newStatus.getId(), oldStatus == null ? null : oldStatus.getState(), newStatus.getState(), System.identityHashCode(newStatus))).printStackTrace();
    }

    public SlotStatus terminate()
    {
        lock();
        try {
            if (!terminated) {
                // verify current actual status
                SlotStatus status = updateStatus();
                checkState(status.getState() == STOPPED || deploymentManager.getDeployment() == null, "Slot is not stopped, but is %s", status.getState());

                // terminate the slot
                deploymentManager.terminate();
                terminated = true;
            }

            SlotStatus slotStatus = this.slotStatus.get().changeState(TERMINATED);
            changeSlotStatus(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    public SlotStatus status()
    {
        return slotStatus.get();
    }

    public SlotStatus updateStatus()
    {
        try {
            lock();
        }
        catch (LockTimeoutException e) {
            // could not get the lock because there is an operation in progress
            // just return the last state we saw
            // todo consider adding "in-process" states like starting
            return slotStatus.get();
        }
        try {
            if (terminated) {
                return slotStatus.get().changeState(TERMINATED);
            }

            Deployment activeDeployment = deploymentManager.getDeployment();
            if (activeDeployment == null) {
                return slotStatus.get().changeAssignment(UNKNOWN, null, ImmutableMap.<String, Integer>of());
            }

            SlotLifecycleState status = lifecycleManager.status(activeDeployment);
            SlotStatus slotStatus = this.slotStatus.get().changeState(status);
            changeSlotStatus(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    public void addStateChangeListener(StateChangeListener<SlotStatus> stateChangeListener)
    {
        slotStatus.addStateChangeListener(stateChangeListener);
    }

    public Duration waitForStateChange(SlotStatus currentState, Duration maxWait)
            throws InterruptedException
    {
        return slotStatus.waitForStateChange(currentState, maxWait);
    }

    public SlotStatus start()
    {
        checkState(!terminated, "Slot has been terminated");

        Deployment activeDeployment = deploymentManager.getDeployment();
        if (activeDeployment == null) {
            throw new IllegalStateException("Slot can not be started because the slot is not assigned");
        }

        SlotLifecycleState state = lifecycleManager.start(activeDeployment);
        checkState(state == RUNNING, "Start failed");
        SlotStatus slotStatus = this.slotStatus.get().changeState(state);
        changeSlotStatus(slotStatus);
        return slotStatus;
    }

    public SlotStatus restart()
    {
        lock();
        try {
            checkState(!terminated, "Slot has been terminated");

            Deployment activeDeployment = deploymentManager.getDeployment();
            if (activeDeployment == null) {
                throw new IllegalStateException("Slot can not be restarted because the slot is not assigned");
            }

            SlotLifecycleState state = lifecycleManager.restart(activeDeployment);

            SlotStatus slotStatus = this.slotStatus.get().changeState(state);
            changeSlotStatus(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    public SlotStatus stop()
    {
        lock();
        try {
            checkState(!terminated, "Slot has been terminated");

            Deployment activeDeployment = deploymentManager.getDeployment();
            if (activeDeployment == null) {
                throw new IllegalStateException("Slot can not be stopped because the slot is not assigned");
            }

            SlotLifecycleState state = lifecycleManager.stop(activeDeployment);

            SlotStatus slotStatus = this.slotStatus.get().changeState(state);
            changeSlotStatus(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    public SlotStatus kill()
    {
        lock();
        try {
            checkState(!terminated, "Slot has been terminated");

            Deployment activeDeployment = deploymentManager.getDeployment();
            if (activeDeployment == null) {
                throw new IllegalStateException("Slot can not be killed because the slot is not assigned");
            }

            SlotLifecycleState state = lifecycleManager.kill(activeDeployment);

            SlotStatus slotStatus = this.slotStatus.get().changeState(state);
            changeSlotStatus(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    private void lock()
    {
        try {
            if (!lock.tryLock((long) lockWait.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new LockTimeoutException(lockOwner, lockWait, lockAcquisitionLocation);
            }

            // capture the location where the lock was acquired
            lockAcquisitionLocation = ImmutableList.copyOf(new Exception("lock acquired HERE").fillInStackTrace().getStackTrace());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    private void unlock()
    {
        lockOwner = null;
        lockAcquisitionLocation = null;
        lock.unlock();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Slot other = (Slot) obj;
        return Objects.equal(this.id, other.id);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("location", location)
                .toString();
    }
}
