package io.airlift.airship.shared.job;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InstallTask.class, name = "install"),
        @JsonSubTypes.Type(value = StartTask.class, name = "start"),
        @JsonSubTypes.Type(value = StopTask.class, name = "stop"),
        @JsonSubTypes.Type(value = RestartTask.class, name = "restart"),
        @JsonSubTypes.Type(value = KillTask.class, name = "kill"),
        @JsonSubTypes.Type(value = TerminateTask.class, name = "terminate")})
public interface Task
{
    String getName();
}
