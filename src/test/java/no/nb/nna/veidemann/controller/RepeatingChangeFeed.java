package no.nb.nna.veidemann.controller;

import no.nb.nna.veidemann.commons.db.ChangeFeed;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A ChangeFeed which repeatedly returns the same value a preconfigured number of times.
 *
 * @param <T>
 */
public class RepeatingChangeFeed<T> implements ChangeFeed<T> {
    int count;
    final T value;

    public RepeatingChangeFeed(int count, T value) {
        this.count = count;
        this.value = value;
    }

    @Override
    public Stream<T> stream() {
        Iterator<T> it = new Iterator<T>() {
            int c = count;

            @Override
            public boolean hasNext() {
                return c > 0;
            }

            @Override
            public T next() {
                c--;
                return value;
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
    }

    @Override
    public void close() {
    }
}
