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
package io.airlift.airship.shared;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;

import static io.airlift.airship.shared.CoordinatorLifecycleState.ONLINE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestCoordinatorStatus
{
    private final JsonCodec<CoordinatorStatus> codec = jsonCodec(CoordinatorStatus.class);

    private final CoordinatorStatus expected = new CoordinatorStatus(
            "44444444-4444-4444-4444-444444444444",
            "4444",
            ONLINE,
            "instanceId",
            URI.create("internal://coordinator"),
            URI.create("external://coordinator"),
            "/test/unknown/location",
            "/unknown/location",
            "instance.type",
            "version"
    );

    @Test
    public void testJsonRoundTrip()
    {
        String json = codec.toJson(expected);
        System.out.println(json);
        CoordinatorStatus actual = codec.fromJson(json);
        assertEquals(actual, expected);
    }

    @Test
    public void testJsonDecode()
            throws Exception
    {
        String json = Resources.toString(Resources.getResource("coordinator-status.json"), Charsets.UTF_8);
        CoordinatorStatus actual = codec.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getCoordinatorId(), expected.getCoordinatorId());
        assertEquals(actual.getShortCoordinatorId(), expected.getShortCoordinatorId());
        assertEquals(actual.getInstanceId(), expected.getInstanceId());
        assertEquals(actual.getInternalUri(), expected.getInternalUri());
        assertEquals(actual.getState(), expected.getState());
        assertEquals(actual.getInstanceType(), expected.getInstanceType());
        assertEquals(actual.getLocation(), expected.getLocation());
        assertEquals(actual.getShortLocation(), expected.getShortLocation());
        assertEquals(actual.getVersion(), expected.getVersion());
    }
}
