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
package com.github.fhuz.kafka.streams.cep.pattern;

import java.util.Iterator;
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

    private String name;

    private Matcher<K, V> predicate;

    private Long windowTime;

    private TimeUnit windowUnit;

    private Pattern<K, V> ancestor;

    private SelectStrategy strategy;

    private Cardinality cardinality = Cardinality.ONE;

    /**
     * Creates a new {@link Pattern} instance.
     **/
    Pattern(String name) {
        this(name, SelectStrategy.STRICT_CONTIGUITY, null);
    }

    /**
     * Creates a new {@link Pattern} instance.
     *
     * @param name     the name of the event.
     * @param strategy the strategy used to select event.
     * @param ancestor the ancestor event pattern.
     */
    private Pattern(String name, SelectStrategy strategy, Pattern<K, V> ancestor) {
        this.name = name;
        this.ancestor = ancestor;
        this.strategy = strategy;
    }

    public Pattern<K, V> withStrategy(SelectStrategy strategy) {
        this.strategy = strategy;
        return this;
    }

    public Pattern<K, V> where(Matcher<K, V> predicate) {
        if (this.predicate == null)
            this.predicate = predicate;
        else
            this.predicate = Matcher.and(this.predicate, predicate);
        return this;
    }

    public Pattern<K, V> within(long time, TimeUnit unit) {
        this.windowTime = time;
        this.windowUnit = unit;
        return this;
    }

    public Pattern<K, V> followBy(String event) {
        return followBy(event, SelectStrategy.STRICT_CONTIGUITY);
    }

    public Pattern<K, V> followBy(String event, SelectStrategy strategy) {
        return new Pattern<>(event, strategy, this);
    }

    public Pattern<K, V> optional() {
        this.cardinality = Cardinality.OPTIONAL;
        return this;
    }

    public Pattern<K, V> oneOrMore() {
        this.cardinality = Cardinality.ONE_OR_MORE;
        return this;
    }

    public Pattern<K, V> zeroOrMore() {
        this.cardinality = Cardinality.ZERO_OR_MORE;
        return this;
    }

    Cardinality getCardinality() {
        return cardinality;
    }

    String getName() {
        return name;
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
