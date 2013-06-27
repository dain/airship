package io.airlift.airship.coordinator.job;

import com.google.common.collect.Iterables;

public class NestedStream<E>
{
    private final Iterable<Iterable<E>> iterable;

    public NestedStream(Iterable<Iterable<E>> iterable)
    {
        this.iterable = iterable;
    }

    public Stream<E> flatten()
    {
        return new Stream<>(Iterables.concat(iterable));
    }
}
