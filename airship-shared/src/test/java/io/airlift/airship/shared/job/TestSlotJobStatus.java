package io.airlift.airship.shared.job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.airship.shared.FailureInfo;
import io.airlift.airship.shared.SlotStatusRepresentation;
import io.airlift.airship.shared.job.SlotJobStatus.SlotJobState;
import io.airlift.airship.shared.job.TaskStatus.TaskState;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static io.airlift.airship.shared.AssignmentHelper.APPLE_ASSIGNMENT;
import static io.airlift.airship.shared.InstallationHelper.APPLE_INSTALLATION;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static org.testng.Assert.assertEquals;

public class TestSlotJobStatus
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        JsonCodec<SlotJobStatus> jsonCodec = JsonCodec.jsonCodec(SlotJobStatus.class);

        SlotStatusRepresentation representation = new SlotStatusRepresentation(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                null,
                URI.create("internal://apple"),
                URI.create("external://apple"),
                "instance",
                "/test/location/apple",
                "/location/apple",
                APPLE_ASSIGNMENT.getBinary(),
                APPLE_ASSIGNMENT.getBinary(),
                APPLE_ASSIGNMENT.getConfig(),
                APPLE_ASSIGNMENT.getConfig(),
                STOPPED.toString(),
                "abc",
                null,
                "/apple",
                ImmutableMap.<String, Integer>of(),
                null,
                null,
                null);

        SlotJobStatus expected = new SlotJobStatus(new SlotJobId("id"),
                SlotJobState.DONE,
                representation,
                "progress",
                42.0,
                ImmutableList.of(
                        new TaskStatus(new InstallTask(APPLE_INSTALLATION), TaskState.DONE, null),
                        new TaskStatus(new StartTask(), TaskState.DONE, null),
                        new TaskStatus(new StopTask(), TaskState.DONE, null),
                        new TaskStatus(new RestartTask(), TaskState.FAILED, FailureInfo.toFailure(new RuntimeException("Message"))),
                        new TaskStatus(new KillTask(), TaskState.SKIPPED, null),
                        new TaskStatus(new TerminateTask(), TaskState.PENDING, null)
                ));

        String json = jsonCodec.toJson(expected);
        SlotJobStatus actual = jsonCodec.fromJson(json);

        assertEquals(actual.getSlotJobId(), expected.getSlotJobId());
        assertEquals(actual.getSlotStatus(), expected.getSlotStatus());
        List<TaskStatus> actualTasks = actual.getTasks();
        List<TaskStatus> expectedTasks = expected.getTasks();
        assertEquals(actualTasks.size(), expectedTasks.size());
        for (int i = 0; i < actualTasks.size(); i++) {
            TaskStatus actualTask = actualTasks.get(i);
            TaskStatus expectedTask = expectedTasks.get(i);
            assertEquals(actualTask.getTask(), expectedTask.getTask());
            assertEquals(actualTask.getState(), expectedTask.getState());
            if (actualTask.getFailureInfo() != null) {
                assertEquals(actualTask.getFailureInfo().getMessage(), expectedTask.getFailureInfo().getMessage());
            } else {
                assertEquals(actualTask.getFailureInfo(), expectedTask.getFailureInfo());
            }
        }
    }

}
