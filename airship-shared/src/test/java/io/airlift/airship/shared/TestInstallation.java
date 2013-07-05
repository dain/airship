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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;

import static io.airlift.airship.shared.AssignmentHelper.APPLE_ASSIGNMENT;
import static io.airlift.airship.shared.AssignmentHelper.BANANA_ASSIGNMENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.EquivalenceTester.equivalenceTester;
import static org.testng.Assert.assertEquals;

public class TestInstallation
{
    private final JsonCodec<Installation> codec = jsonCodec(Installation.class);

    private final Installation expected = new Installation(
            APPLE_ASSIGNMENT,
            URI.create("fetch://binary.tar.gz"),
            URI.create("fetch://config.config"),
            ImmutableMap.of("memory", 512)
    );

    @Test
    public void testConstructor()
    {
        URI binaryFile = URI.create("fake://localhost/binaryFile");
        URI configFile = URI.create("fake://localhost/configFile");

        Installation installation = new Installation(APPLE_ASSIGNMENT, binaryFile, configFile, ImmutableMap.of("memory", 512));

        assertEquals(installation.getAssignment(), APPLE_ASSIGNMENT);
        assertEquals(installation.getBinaryFile(), binaryFile);
        assertEquals(installation.getConfigFile(), configFile);
    }

    @Test
    public void testJsonRoundTrip()
    {
        String json = codec.toJson(expected);
        Installation actual = codec.fromJson(json);
        assertEquals(actual, expected);
    }

    @Test
    public void testJsonDecode()
            throws Exception
    {
        String json = Resources.toString(Resources.getResource("installation.json"), Charsets.UTF_8);
        Installation actual = codec.fromJson(json);

        assertEquals(actual, expected);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                .addEquivalentGroup(new Installation(APPLE_ASSIGNMENT,
                        URI.create("fetch://binary.tar.gz"),
                        URI.create("fetch://config.txt"),
                        ImmutableMap.of("memory", 512)))
                .addEquivalentGroup(new Installation(APPLE_ASSIGNMENT,
                        URI.create("fetch://anything.tar.gz"),
                        URI.create("fetch://anything.txt"),
                        ImmutableMap.of("memory", 512)))
                .addEquivalentGroup(new Installation(BANANA_ASSIGNMENT,
                        URI.create("fetch://binary.tar.gz"),
                        URI.create("fetch://config.txt"),
                        ImmutableMap.of("memory", 512)))
                .addEquivalentGroup(new Installation(BANANA_ASSIGNMENT,
                        URI.create("fetch://anything.tar.gz"),
                        URI.create("fetch://anything.txt"),
                        ImmutableMap.of("memory", 512)))
                .check();
    }
}
