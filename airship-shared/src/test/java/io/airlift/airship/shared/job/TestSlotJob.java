package io.airlift.airship.shared.job;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static io.airlift.airship.shared.InstallationHelper.APPLE_INSTALLATION;
import static org.testng.Assert.assertEquals;

public class TestSlotJob
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        JsonCodec<SlotJob> jsonCodec = JsonCodec.jsonCodec(SlotJob.class);

        SlotJob expected = new SlotJob(new SlotJobId("id"), UUID.randomUUID(), ImmutableList.of(
                new InstallTask(APPLE_INSTALLATION),
                new StartTask(),
                new StopTask(),
                new RestartTask(),
                new KillTask(),
                new TerminateTask()
        ));

        String json = jsonCodec.toJson(expected);
        SlotJob actual = jsonCodec.fromJson(json);

        assertEquals(actual.getSlotJobId(), expected.getSlotJobId());
        assertEquals(actual.getSlotId(), expected.getSlotId());
        List<Task> actualTasks = actual.getTasks();
        List<Task> expectedTasks = expected.getTasks();
        assertEquals(actualTasks.size(), expectedTasks.size());
        for (int i = 0; i < actualTasks.size(); i++) {
            Task actualTask = actualTasks.get(i);
            Task expectedTask = expectedTasks.get(i);
            assertEquals(actualTask, expectedTask);
        }
    }
}
