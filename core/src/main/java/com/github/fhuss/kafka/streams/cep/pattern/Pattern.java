/*
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
        ONE_OR_MORE(-1),
        ONE(1);

        private int val;

        Cardinality(int val) {
            this.val = val;
        }

        public int value() {
            return val;
        }
    }

    private int level;

    private String name;

    private Matcher<K, V> predicate;

    private Long windowTime;

    private TimeUnit windowUnit;

    private Pattern<K, V> ancestor;

    private List<StateAggregator<K, V, Object>> aggregates;

    private Cardinality cardinality = Cardinality.ONE;

    private Selected selected;

    private boolean isOptional = false;

    private int times = 1;

    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern(final Selected selected) {
        this(null, selected);
    }

    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern(final String name, final Selected selected) {
        this(0, name, selected);
    }

    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern(final int level, final String name, final Selected selected) {
        this(level, name, selected, null);
    }

    /**
     * Creates a new {@link Pattern} instance.
     *
     * @param ancestor the ancestor event pattern.
     */
    Pattern(final Pattern<K, V> ancestor) {
        this(ancestor.level + 1, null, null, ancestor);
    }

    /**
     * Creates a new {@link Pattern} instance.
     *
     * @param level    the level of this pattern
     * @param name     the name of the event
     * @param selected
     * @param ancestor the ancestor event pattern
     */
    private Pattern(final int level, final String name, final Selected selected, final Pattern<K, V> ancestor) {
        this.level     = level;
        this.name      = name;
        this.ancestor  = ancestor;
        this.aggregates = new ArrayList<>();
        this.selected = selected == null ? Selected.withStrictContiguity() : selected;
    }

    public StageBuilder<K, V> select() {
        return new StageBuilder<>(this);
    }

    public StageBuilder<K, V> select(final Selected selected) {
        this.selected = selected;
        return new StageBuilder<>(this);
    }

    public StageBuilder<K, V> select(final String name) {
        this.name = name;
        return new StageBuilder<>(this);
    }

    public StageBuilder<K, V> select(final String name, final Selected selected) {
        this.name = name;
        this.selected = selected;
        return new StageBuilder<>(this);
    }

    boolean isOptional() {
        return isOptional;
    }

    void setOptional(boolean optional) {
        isOptional = optional;
    }

    void setTimes(int times) {
        this.times = times;
    }

    int getTimes() {
        return times;
    }

    @SuppressWarnings("unchecked")
    <T> Pattern<K, V> addStateAggregator(final StateAggregator<K, V, T> aggregator) {
        this.aggregates.add((StateAggregator<K, V, Object>)aggregator);
        return this;
    }

    /**
     * @deprecated use {@link QueryBuilder#select(Selected)} to specify the event selection strategy to apply.
     */
    @Deprecated
    void setStrategy(final Strategy strategy) {
        this.selected = this.selected.withStrategy(strategy);
    }

    void setWindow(long time, TimeUnit unit) {
        this.windowTime = time;
        this.windowUnit = unit;
    }

    void andPredicate(final Matcher<K, V> predicate) {
        if (this.predicate == null)
            this.predicate = predicate;
        else
            this.predicate = Matcher.and(this.predicate, predicate);
    }

    void orPredicate(final Matcher<K, V> predicate) {
        if (this.predicate == null)
            this.predicate = predicate;
        else
            this.predicate = Matcher.or(this.predicate, predicate);
    }



    void setCardinality(final Cardinality cardinality) {
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

    Selected getSelected() {
        return selected;
    }

    /**
     * @deprecated use {@link Pattern#getSelected()}.
     */
    @Deprecated
    Strategy getStrategy() {
        return selected.getStrategy();
    }

    List<StateAggregator<K, V, Object>> getAggregates() { return this.aggregates; }

    @Override
    public Iterator<Pattern<K, V>> iterator() {
        return new PatternIterator<>(this);
    }

    private static class PatternIterator<K, V> implements Iterator<Pattern<K, V>> {
        private Pattern<K, V> current;

        PatternIterator(Pattern<K, V> pattern) {
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