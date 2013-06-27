package io.airlift.airship.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class IdAndVersion
{
    private final String id;
    private final String version;

    @JsonCreator
    public IdAndVersion(@JsonProperty("id") String id, @Nullable @JsonProperty("version") String version)
    {
        this.id = id;
        this.version = version;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @Nullable
    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    public static Function<IdAndVersion, String> idGetter()
    {
        return new Function<IdAndVersion, String>()
        {
            @Override
            public String apply(IdAndVersion input)
            {
                return input.getId();
            }
        };
    }

    public static List<IdAndVersion> forIds(String... ids)
    {
        ImmutableList.Builder<IdAndVersion> builder = ImmutableList.builder();
        for (String id : ids) {
            builder.add(new IdAndVersion(id, null));
        }

        return builder.build();
    }

    public static List<IdAndVersion> forIds(UUID... ids)
    {
        ImmutableList.Builder<IdAndVersion> builder = ImmutableList.builder();
        for (UUID id : ids) {
            builder.add(new IdAndVersion(id.toString(), null));
        }

        return builder.build();
    }

}
