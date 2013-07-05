package io.airlift.airship.shared;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.airship.shared.SlotLifecycleState.STOPPED;
import static org.testng.Assert.assertEquals;

public class TestSlotStatusJson
{
    private final JsonCodec<SlotStatus> codec = jsonCodec(SlotStatus.class);

    private final SlotStatus expected = new SlotStatus(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            "aaaaa",
            URI.create("internal://apple"),
            URI.create("external://apple"),
            "instance",
            "/test/location/apple",
            "/location/apple",
            STOPPED,
            AssignmentHelper.APPLE_ASSIGNMENT,
            "/apple",
            ImmutableMap.of("memory", 512),
            STOPPED,
            AssignmentHelper.APPLE_ASSIGNMENT,
            "status message",
            "version");

    @Test
    public void testJsonRoundTrip()
    {
        String json = codec.toJson(expected);
        SlotStatus actual = codec.fromJson(json);
        assertEquals(actual, expected);
    }

    @Test
    public void testJsonDecode()
            throws Exception
    {
        String json = Resources.toString(Resources.getResource("slot-status.json"), Charsets.UTF_8);
        SlotStatus actual = codec.fromJson(json);

        assertEquals(actual, expected);
    }
}
