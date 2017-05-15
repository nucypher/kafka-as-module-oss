package com.nucypher.kafka;

import java.io.Serializable;
import java.util.Objects;

/**
 */
public class Pair<F, L> implements Serializable {

    private final F first;
    private final L last;

    public Pair(F first, L last) {
        this.first = first;
        this.last = last;
    }

    public F getFirst() {
        return first;
    }

    public L getLast() {
        return last;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", last=" + last +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) &&
                Objects.equals(last, pair.last);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, last);
    }
}
