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

import java.util.concurrent.TimeUnit;

public class PatternBuilder<K, V> {

    private final Pattern<K, V> pattern;

    /**
     * Creates a  new {@link PatternBuilder} instance.
     *
     * @param pattern the current pattern
     */
    PatternBuilder(final Pattern<K, V> pattern) {
        this.pattern = pattern;
    }

    public PatternBuilder<K, V> and(final SimpleMatcher<K, V> predicate) {
        this.pattern.andPredicate(predicate);
        return this;
    }

    public PatternBuilder<K, V> and(final StatefulMatcher<K, V> predicate) {
        this.pattern.andPredicate(predicate);
        return this;
    }

    public PatternBuilder<K, V> and(final SequenceMatcher<K, V> predicate) {
        this.pattern.andPredicate(predicate);
        return this;
    }

    public PatternBuilder<K, V> or(final SimpleMatcher<K, V> predicate) {
        this.pattern.orPredicate(predicate);
        return this;
    }

    public PatternBuilder<K, V> or(final StatefulMatcher<K, V> predicate) {
        this.pattern.orPredicate(predicate);
        return this;
    }

    public PatternBuilder<K, V> or(final SequenceMatcher<K, V> predicate) {
        this.pattern.orPredicate(predicate);
        return this;
    }

    public <T> PatternBuilder<K, V> fold(final String state, final Aggregator<K, V, T> aggregator) {
        this.pattern.addStateAggregator(new StateAggregator<>(state, aggregator));
        return this;
    }

    public PatternBuilder<K, V> within(long time, TimeUnit unit) {
        this.pattern.setWindow(time, unit);
        return this;
    }

    public Pattern<K, V> then() {
        return new Pattern<>(pattern);
    }

    public Pattern<K, V> build() {
        return pattern;
    }
}
