/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.core.pattern;

public class PredicateBuilder<K, V> {

    protected final Pattern<K, V> pattern;

    /**
     * Creates a new {@link PredicateBuilder} instance.
     *
     * @param pattern the current pattern
     */
    PredicateBuilder(final Pattern<K, V> pattern) {
        this.pattern = pattern;
    }

    public PatternBuilder<K, V> where(final SimpleMatcher<K, V> predicate) {
        this.pattern.andPredicate(predicate);
        return new PatternBuilder<>(this.pattern);
    }

    public PatternBuilder<K, V> where(final StatefulMatcher<K, V> predicate) {
        this.pattern.andPredicate(predicate);
        return new PatternBuilder<>(this.pattern);
    }

    public PatternBuilder<K, V> where(final SequenceMatcher<K, V> predicate) {
        this.pattern.andPredicate(predicate);
        return new PatternBuilder<>(this.pattern);
    }

    public PredicateBuilder<K, V> optional() {
        this.pattern.setOptional(true);
        return this;
    }
}
