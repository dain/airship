package io.airlift.airship.shared;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestAssignment
{
    private final JsonCodec<Assignment> codec = jsonCodec(Assignment.class);

    private final Assignment expected = new Assignment("1.1", "2.2");

    @Test
    public void testJsonRoundTrip()
    {
        String json = codec.toJson(expected);
        Assignment actual = codec.fromJson(json);
        assertAssignmentsEqual(actual, expected);
    }

    @Test
    public void testJsonDecode()
            throws Exception
    {
        String json = "{\"binary\":\"1.1\",\"config\":\"2.2\"}";
        Assignment actual = codec.fromJson(json);

        assertAssignmentsEqual(actual, expected);
    }

    private void assertAssignmentsEqual(Assignment actual, Assignment expected)
    {
        assertEquals(actual.getBinary(), expected.getBinary());
        assertEquals(actual.getConfig(), this.expected.getConfig());
    }
}
