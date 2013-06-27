package io.airlift.airship.coordinator.job;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class Stream<E>
{
    private final Iterable<E> iterable;

    Stream(Iterable<E> iterable)
    {
        this.iterable = iterable;
    }

    public static <T> Stream<T> on(Iterable<T> iterable)
    {
        return new Stream<>(iterable);
    }

    public <T> Stream<T> transform(Function<? super E, T> function)
    {
        return new Stream<>(Iterables.transform(iterable, function));
    }

    public <T> Stream<T> cast(final Class<T> clazz)
    {
        return new Stream<>(Iterables.transform(iterable, new Function<E, T>()
        {
            @Override
            public T apply(E input)
            {
                return clazz.cast(input);
            }
        }));
    }

    public <T> Stream<T> transformAndFlatten(Function<? super E, ? extends Iterable<T>> function)
    {
        return new Stream<>(Iterables.concat(Iterables.transform(iterable, function)));
    }

    public <T> NestedStream<T> transformNested(Function<? super E, ? extends Iterable<T>> function)
    {
        return new NestedStream<>(Iterables.transform(iterable, function));
    }

    public Stream<E> select(Predicate<? super E> predicate)
    {
        return new Stream<>(Iterables.filter(iterable, predicate));
    }

    public Stream<E> orderBy(Comparator<E> ordering)
    {
        return new Stream<>(Ordering.from(ordering).sortedCopy(iterable));
    }

    public boolean all(Predicate<E> predicate)
    {
        return Iterables.all(iterable, predicate);
    }

    public boolean any(Predicate<E> predicate)
    {
        return Iterables.any(iterable, predicate);
    }

    public List<E> list()
    {
        return ImmutableList.copyOf(iterable);
    }

    public Set<E> set()
    {
        return ImmutableSet.copyOf(iterable);
    }

    public Multiset<E> bag()
    {
        return ImmutableMultiset.copyOf(iterable);
    }

    public Iterable<E> all()
    {
        return iterable;
    }

    public E first()
    {
        return Iterables.getFirst(iterable, null);
    }

    public E only()
    {
        return Iterables.getOnlyElement(iterable);
    }

    public E last()
    {
        return Iterables.getLast(iterable);
    }
}
