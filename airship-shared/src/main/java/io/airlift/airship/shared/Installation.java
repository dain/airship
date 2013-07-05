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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.Map;

@Immutable
public class Installation
{
    private final Assignment assignment;
    private final URI binaryFile;
    private final URI configFile;
    private final Map<String, Integer> resources;

    @JsonCreator
    public Installation(
            @JsonProperty("assignment") Assignment assignment,
            @JsonProperty("binaryFile") URI binaryFile,
            @JsonProperty("configFile") URI configFile,
            @JsonProperty("resources") Map<String, Integer> resources)
    {
        Preconditions.checkNotNull(assignment, "assignment is null");
        Preconditions.checkNotNull(binaryFile, "binaryFile is null");
        Preconditions.checkNotNull(configFile, "configFile is null");
        Preconditions.checkNotNull(resources, "resources is null");

        this.assignment = assignment;
        this.binaryFile = binaryFile;
        this.configFile = configFile;
        this.resources = ImmutableMap.copyOf(resources);
    }

    @JsonProperty
    public Assignment getAssignment()
    {
        return assignment;
    }

    @JsonProperty
    public URI getBinaryFile()
    {
        return binaryFile;
    }

    @JsonProperty
    public URI getConfigFile()
    {
        return configFile;
    }

    @JsonProperty
    public Map<String, Integer> getResources()
    {
        return resources;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(assignment, binaryFile, configFile, resources);
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
        final Installation other = (Installation) obj;
        return Objects.equal(this.assignment, other.assignment) && Objects.equal(this.binaryFile, other.binaryFile) && Objects.equal(this.configFile,
                other.configFile) && Objects.equal(this.resources, other.resources);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("assignment", assignment)
                .add("binaryFile", binaryFile)
                .add("configFile", configFile)
                .add("resources", resources)
                .toString();
    }
}
