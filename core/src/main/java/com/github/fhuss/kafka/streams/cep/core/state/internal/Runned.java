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
package com.github.fhuss.kafka.streams.cep.core.state.internal;

import org.apache.commons.lang3.builder.CompareToBuilder;

import java.io.Serializable;
import java.util.Objects;

public class Runned<K> implements Comparable<Runned>, Serializable {

    private K key;

    /**
     * Creates a new {@link Runned} instance.
     * @param key   the key used to group events.
     */
    public Runned(final K key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Runned<?> runned = (Runned<?>) o;
        return Objects.equals(key, runned.key);
    }

    public K getKey() {
        return key;
    }

    @Override
    public int hashCode() {

        return Objects.hash(key);
    }

    @Override
    public int compareTo(final Runned that) {
        CompareToBuilder compareToBuilder = new CompareToBuilder();
        return compareToBuilder.append(this.key, that.key).build();
    }

    @Override
    public String toString() {
        return "Runned{" +
                "key=" + key +
                '}';
    }
}