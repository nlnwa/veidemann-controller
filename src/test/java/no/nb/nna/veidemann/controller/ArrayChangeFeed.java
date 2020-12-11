package no.nb.nna.veidemann.controller;

import no.nb.nna.veidemann.commons.db.ChangeFeed;

import java.util.Arrays;
import java.util.stream.Stream;

public class ArrayChangeFeed<T> implements ChangeFeed<T> {
    final T[] values;

    public ArrayChangeFeed(T... values) {
        this.values = values;
    }

    @Override
    public Stream<T> stream() {
        return Arrays.stream(values);
    }

    @Override
    public void close() {
    }
}
