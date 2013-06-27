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
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Immutable
public class Assignment
{
    private final String binary;
    private final String config;

    @JsonCreator
    public Assignment(@JsonProperty("binary") String binary, @JsonProperty("config") String config)
    {
        this.binary = binary;
        this.config = config;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignment that = (Assignment) o;

        if (!binary.equals(that.binary)) {
            return false;
        }
        if (!config.equals(that.config)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = binary.hashCode();
        result = 31 * result + config.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Assignment");
        sb.append("{binary=").append(binary);
        sb.append(", config=").append(config);
        sb.append('}');
        return sb.toString();
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

        return new Assignment(newBinary, newConfig);
    }

    public Assignment forceAssignment(Repository repository)
    {
        checkState((binary != null) && (config != null), "Binary and config must be specified to upgrade missing assignment");

        String newBinary = repository.binaryResolve(binary);
        checkArgument(newBinary != null, "Unknown binary " + binary);

        String newConfig = repository.configResolve(config);
        checkArgument(newConfig != null, "Unknown config " + config);

        return new Assignment(newBinary, newConfig);
    }
}
