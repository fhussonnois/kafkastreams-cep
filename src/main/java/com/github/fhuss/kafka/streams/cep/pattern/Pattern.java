/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.pattern;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public class Pattern<K, V> implements Iterable<Pattern<K, V>> {

    public enum Cardinality {
        ZERO_OR_MORE(-2),
        ONE_OR_MORE(-1),
        OPTIONAL(0),
        ONE(1);

        private int val;

        Cardinality(int val) {
            this.val = val;
        }

        public int value() {
            return val;
        }
    }

    public enum SelectStrategy {
        /**
         * This strategy enforce the selected events to be contiguous in the input stream.
         */
        STRICT_CONTIGUITY,
        /**
         * This strategy allow to skip irrelevant events until a the next relevant event is selected.
         */
        SKIP_TIL_NEXT_MATCH,
        /**
         *
         */
        SKIP_TIL_ANY_MATCH
    }

    private int level;

    private String name;

    private Matcher<K, V> predicate;

    private Long windowTime;

    private TimeUnit windowUnit;

    private Pattern<K, V> ancestor;

    private SelectStrategy strategy;

    private List<StateAggregator<K, V, Object>> aggregates;

    private Cardinality cardinality = Cardinality.ONE;

    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern() {
        this(0, null);
    }
    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern(String name) {
        this(0, name);
    }

    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern(int level, String name) {
        this(level, name, null);
    }

    /**
     * Creates a new {@link Pattern} instance.
     *
     * @param ancestor the ancestor event pattern.
     */
    Pattern(Pattern<K, V> ancestor) {
        this(ancestor.level + 1, null, ancestor);
    }

    /**
     * Creates a new {@link Pattern} instance.
     *
     * @param name     the name of the event.
     * @param ancestor the ancestor event pattern.
     */
    private Pattern(int level, String name, Pattern<K, V> ancestor) {
        this.level     = level;
        this.name      = name;
        this.ancestor  = ancestor;
        this.strategy  = SelectStrategy.STRICT_CONTIGUITY;
        this.aggregates = new ArrayList<>();
    }


    public SelectBuilder<K, V> select() {
        return new SelectBuilder<>(this);
    }

    public SelectBuilder<K, V> select(String name) {
        this.name = name;
        return new SelectBuilder<>(this);
    }

    @SuppressWarnings("unchecked")
    <T> Pattern<K, V> addStateAggregator(StateAggregator<K, V, T> aggregator) {
        this.aggregates.add((StateAggregator<K, V, Object>)aggregator);
        return this;
    }

    void setStrategy(SelectStrategy strategy) {
        this.strategy = strategy;
    }

    void setWindow(long time, TimeUnit unit) {
        this.windowTime = time;
        this.windowUnit = unit;
    }

    void addPredicate(Matcher<K, V> predicate) {
        if (this.predicate == null)
            this.predicate = predicate;
        else
            this.predicate = Matcher.and(this.predicate, predicate);
    }

    void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    Cardinality getCardinality() {
        return cardinality;
    }

    String getName() {
        return (name == null) ? String.valueOf(this.level) : name;
    }

    Matcher<K, V> getPredicate() {
        return predicate;
    }

    Long getWindowTime() {
        return windowTime;
    }

    TimeUnit getWindowUnit() {
        return windowUnit;
    }

    Pattern<K, V> getAncestor() {
        return ancestor;
    }

    SelectStrategy getStrategy() {
        return strategy;
    }

    List<StateAggregator<K, V, Object>> getAggregates() { return this.aggregates; }

    @Override
    public Iterator<Pattern<K, V>> iterator() {
        return new PatternIterator<>(this);
    }

    private static class PatternIterator<K, V> implements Iterator<Pattern<K, V>> {
        private Pattern<K, V> current;

        public PatternIterator(Pattern<K, V> pattern) {
            this.current = pattern;
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Pattern<K, V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Pattern<K, V> next = current;
            current = next.getAncestor();
            return next;
        }
    }
}
