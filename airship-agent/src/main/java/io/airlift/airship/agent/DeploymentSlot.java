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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.airship.shared.Installation;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import io.airlift.airship.shared.StateMachine;
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

public class DeploymentSlot
        implements Slot
{
    private static final Logger log = Logger.get(DeploymentSlot.class);

    private final UUID id;
    private final String location;
    private final URI self;
    private final URI externalUri;
    private final Duration updateInterval;
    private final Duration lockWait;
    private final DeploymentManager deploymentManager;
    private final LifecycleManager lifecycleManager;
    private final StateMachine<SlotStatus> lastSlotStatus;
    private final Executor executor;
    private boolean terminated;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile Thread lockOwner;
    private volatile List<StackTraceElement> lockAcquisitionLocation;

    public static DeploymentSlot createNewDeploymentSlot(URI self,
            URI externalUri,
            DeploymentManager deploymentManager,
            LifecycleManager lifecycleManager,
            Installation installation,
            Duration updateInterval,
            Duration maxLockWait,
            Executor executor)
    {
        DeploymentSlot deploymentSlot = new DeploymentSlot(self, externalUri, deploymentManager, lifecycleManager, updateInterval, maxLockWait, executor);

        // install the software
        try {
            deploymentSlot.assign(installation, new Progress());
            deploymentSlot.startUpdateLoop();
            return deploymentSlot;
        }
        catch (Exception e) {
            deploymentSlot.terminate();
            throw Throwables.propagate(e);
        }

    }

    public static DeploymentSlot loadDeploymentSlot(URI self,
            URI externalUri,
            DeploymentManager deploymentManager,
            LifecycleManager lifecycleManager,
            Duration updateInterval,
            Duration maxLockWait,
            Executor executor)
    {
        DeploymentSlot deploymentSlot = new DeploymentSlot(self, externalUri, deploymentManager, lifecycleManager, updateInterval, maxLockWait, executor);
        deploymentSlot.startUpdateLoop();
        return deploymentSlot;
    }

    private DeploymentSlot(URI self,
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
        this.lastSlotStatus = new StateMachine<>("slot " + id, executor, null);
        this.executor = executor;

        Deployment deployment = deploymentManager.getDeployment();
        if (deployment == null) {
            lastSlotStatus.set(createSlotStatus(id,
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
            lastSlotStatus.set(createSlotStatus(id,
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

    public void startUpdateLoop()
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

    @Override
    public UUID getId()
    {
        return id;
    }

    @Override
    public URI getSelf()
    {
        return self;
    }

    @Override
    public URI getExternalUri()
    {
        return externalUri;
    }

    @Override
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

            lastSlotStatus.set(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    @Override
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

            SlotStatus slotStatus = lastSlotStatus.get().changeState(TERMINATED);
            lastSlotStatus.set(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    @Override
    public SlotStatus getLastSlotStatus()
    {
        return lastSlotStatus.get();
    }

    @Override
    public SlotStatus status()
    {
        return lastSlotStatus.get();
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
            return lastSlotStatus.get();
        }
        try {
            if (terminated) {
                return lastSlotStatus.get().changeState(TERMINATED);
            }

            Deployment activeDeployment = deploymentManager.getDeployment();
            if (activeDeployment == null) {
                return lastSlotStatus.get().changeAssignment(UNKNOWN, null, ImmutableMap.<String, Integer>of());
            }

            SlotLifecycleState status = lifecycleManager.status(activeDeployment);
            SlotStatus slotStatus = lastSlotStatus.get().changeState(status);
            lastSlotStatus.set(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    @Override
    public SlotStatus start()
    {
        checkState(!terminated, "Slot has been terminated");

        Deployment activeDeployment = deploymentManager.getDeployment();
        if (activeDeployment == null) {
            throw new IllegalStateException("Slot can not be started because the slot is not assigned");
        }

        SlotLifecycleState state = lifecycleManager.start(activeDeployment);
        checkState(state == RUNNING, "Start failed");
        SlotStatus slotStatus = lastSlotStatus.get().changeState(state);
        lastSlotStatus.set(slotStatus);
        return slotStatus;
    }

    @Override
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

            SlotStatus slotStatus = lastSlotStatus.get().changeState(state);
            lastSlotStatus.set(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    @Override
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

            SlotStatus slotStatus = lastSlotStatus.get().changeState(state);
            lastSlotStatus.set(slotStatus);
            return slotStatus;
        }
        finally {
            unlock();
        }
    }

    @Override
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

            SlotStatus slotStatus = lastSlotStatus.get().changeState(state);
            lastSlotStatus.set(slotStatus);
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DeploymentSlot slot = (DeploymentSlot) o;

        if (!id.equals(slot.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Slot");
        sb.append("{slotId=").append(id);
        sb.append(", location='").append(location).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
