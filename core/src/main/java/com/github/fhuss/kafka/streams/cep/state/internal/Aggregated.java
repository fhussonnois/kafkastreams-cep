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
package com.github.fhuss.kafka.streams.cep.state.internal;

import java.util.Objects;

public class Aggregated<K> {

    private final K key;

    private final Aggregate aggregate;

    /**
     * Creates a new {@link Aggregated} instance.
     * @param key
     * @param aggregate
     */
    public Aggregated(final K key, final Aggregate aggregate) {
        this.key = key;
        this.aggregate = aggregate;
    }

    public K getKey() {
        return key;
    }

    public Aggregate getAggregate() {
        return aggregate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Aggregated<?> that = (Aggregated<?>) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(aggregate, that.aggregate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(key, aggregate);
    }

    @Override
    public String toString() {
        return "Aggregated{" +
                "key=" + key +
                ", aggregate=" + aggregate +
                '}';
    }
}
