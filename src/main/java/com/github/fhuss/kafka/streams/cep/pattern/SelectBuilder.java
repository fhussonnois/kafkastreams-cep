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

public class SelectBuilder<K, V> {

    private final Pattern<K, V> pattern;

    public SelectBuilder(Pattern<K, V> pattern) {
        this.pattern = pattern;
    }
    public SelectBuilder<K, V> optional() {
        pattern.setCardinality(Pattern.Cardinality.OPTIONAL);
        return this;
    }

    public SelectBuilder<K, V> oneOrMore() {
        pattern.setCardinality(Pattern.Cardinality.ONE_OR_MORE);
        return this;
    }

    public SelectBuilder<K, V> zeroOrMore() {
        pattern.setCardinality(Pattern.Cardinality.ZERO_OR_MORE);
        return this;
    }

    public SelectBuilder<K, V> skipTillNextMatch() {
        this.pattern.setStrategy(Pattern.SelectStrategy.SKIP_TIL_NEXT_MATCH);
        return this;
    }

    public SelectBuilder<K, V> skipTillAnyMatch() {
        this.pattern.setStrategy(Pattern.SelectStrategy.SKIP_TIL_ANY_MATCH);
        return this;
    }

    public SelectBuilder<K, V> strictContiguity() {
        this.pattern.setStrategy(Pattern.SelectStrategy.STRICT_CONTIGUITY);
        return this;
    }

    public PredicateBuilder<K, V> where(Matcher<K, V> predicate) {
        this.pattern.addPredicate(predicate);
        return new PredicateBuilder<>(this.pattern);
    }

}
