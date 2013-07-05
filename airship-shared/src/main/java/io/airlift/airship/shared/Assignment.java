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

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Immutable
public class Assignment
{
    private final String binary;
    private final String config;

    private final String shortBinary;
    private final String shortConfig;

    public Assignment(String binary, String config)
    {
        this.binary = binary;
        this.config = config;
        this.shortBinary = binary;
        this.shortConfig = config;
    }

    @JsonCreator
    public Assignment(
            @JsonProperty("binary") String binary,
            @JsonProperty("config") String config,
            @JsonProperty("shortBinary") String shortBinary,
            @JsonProperty("shortConfig") String shortConfig)
    {
        this.binary = binary;
        this.config = config;
        this.shortBinary = shortBinary;
        this.shortConfig = shortConfig;
    }

    @JsonProperty
    public String getBinary()
    {
        return binary;
    }

    @JsonProperty
    public String getConfig()
    {
        return config;
    }

    @JsonProperty
    public String getShortBinary()
    {
        return shortBinary;
    }

    @JsonProperty
    public String getShortConfig()
    {
        return shortConfig;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(binary, config);
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
        final Assignment other = (Assignment) obj;
        return Objects.equal(this.binary, other.binary) &&
                Objects.equal(this.config, other.config);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("binary", binary)
                .add("config", config)
                .toString();
    }

    public Assignment upgradeAssignment(Repository repository, Assignment assignment)
    {
        Preconditions.checkNotNull(assignment, "assignment is null");

        String newBinary = assignment.getBinary();
        if (binary != null) {
            newBinary = repository.binaryUpgrade(newBinary, binary);
            checkArgument(newBinary != null, "Can not upgrade binary " + assignment.getBinary() + " to " + binary);
        }
        else {
            checkArgument(repository.binaryToHttpUri(assignment.getBinary()) != null, "Can not locate existing binary " + assignment.getBinary() + " for upgrade");
        }

        String newConfig = assignment.getConfig();
        if (config != null) {
            newConfig = repository.configUpgrade(newConfig, config);
            checkArgument(newConfig != null, "Can not upgrade config " + assignment.getConfig() + " to " + config);
        }
        else {
            checkArgument(repository.configToHttpUri(assignment.getConfig()) != null, "Can not locate existing config " + assignment.getConfig() + " for upgrade");
        }

        return new Assignment(newBinary,
                newConfig,
                relativizeBinary(repository, newBinary),
                relativizeConfig(repository, newConfig));
    }

    public Assignment forceAssignment(Repository repository)
    {
        checkState((binary != null) && (config != null), "Binary and config must be specified to upgrade missing assignment");

        String newBinary = repository.binaryResolve(binary);
        checkArgument(newBinary != null, "Unknown binary " + binary);

        String newConfig = repository.configResolve(config);
        checkArgument(newConfig != null, "Unknown config " + config);

        return new Assignment(newBinary, 
                newConfig, 
                relativizeBinary(repository, newBinary),
                relativizeConfig(repository, newConfig));
    }

    public static Assignment shortenAssignment(Repository repository, Assignment assignment)
    {
        if (assignment == null) {
            return null;
        }

        return new Assignment(
                assignment.binary,
                assignment.config,
                relativizeBinary(repository, assignment.binary),
                relativizeConfig(repository, assignment.config));
    }

    private static String relativizeBinary(Repository repository, String binary)
    {
        if (repository == null) {
            return binary;
        }
        return Objects.firstNonNull(repository.binaryRelativize(binary), binary);
    }

    private static String relativizeConfig(Repository repository, String config)
    {
        if (repository == null) {
            return config;
        }
        return Objects.firstNonNull(repository.configRelativize(config), config);
    }
}
