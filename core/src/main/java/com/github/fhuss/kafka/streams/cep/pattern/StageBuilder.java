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

public class StageBuilder<K, V> extends PredicateBuilder<K, V> {

    /**
     * Creates a new {@link StageBuilder} instance.
     * @param pattern
     */
    StageBuilder(final Pattern<K, V> pattern) {
        super(pattern);
    }

    public PredicateBuilder<K, V> oneOrMore() {
        this.pattern.setCardinality(Pattern.Cardinality.ONE_OR_MORE);
        return this;
    }

    public PredicateBuilder<K, V> zeroOrMore() {
        this.pattern.setCardinality(Pattern.Cardinality.ONE_OR_MORE);
        this.pattern.setOptional(true);
        return this;
    }

    public PredicateBuilder<K, V> times(int times) {
        this.pattern.setTimes(times);
        return this;
    }

    @Deprecated
    public StageBuilder<K, V> skipTillNextMatch() {
        this.pattern.setStrategy(Strategy.SKIP_TIL_NEXT_MATCH);
        return this;
    }

    @Deprecated
    public StageBuilder<K, V> skipTillAnyMatch() {
        this.pattern.setStrategy(Strategy.SKIP_TIL_ANY_MATCH);
        return this;
    }

    @Deprecated
    public StageBuilder<K, V> strictContiguity() {
        this.pattern.setStrategy(Strategy.STRICT_CONTIGUITY);
        return this;
    }
}
