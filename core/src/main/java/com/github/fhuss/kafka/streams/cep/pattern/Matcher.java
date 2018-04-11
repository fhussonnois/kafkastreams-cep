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

import com.github.fhuss.kafka.streams.cep.state.States;

@FunctionalInterface
public interface Matcher<K, V> {

    boolean matches(K key, V value, long timestamp, States store);

    static <K, V> Matcher<K, V> not(Matcher<K, V> predicate) {
        return (key, value, timestamp, stateStore) -> !predicate.matches(key, value, timestamp, stateStore);
    }

    static <K, V> Matcher<K, V> or(Matcher<K, V> left, Matcher<K, V> right) {
        return new OrPredicate<>(left, right);
    }

    static <K, V> Matcher<K, V> and(Matcher<K, V> left, Matcher<K, V> right) {
        return new AndPredicate<>(left, right);
    }

    class AndPredicate<K, V> implements Matcher<K, V> {
        private Matcher<K, V> left;
        private Matcher<K, V> right;

        public AndPredicate(Matcher<K, V> left, Matcher<K, V> right) {
            this.left = left;
            this.right = right;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean matches(K key, V value, long timestamp, States store) {
            return left.matches(key, value, timestamp, store) && right.matches(key, value, timestamp, store);
        }
    }

    class OrPredicate<K, V> implements Matcher<K, V> {
        private Matcher<K, V> left;
        private Matcher<K, V> right;

        public OrPredicate(Matcher<K, V> left, Matcher<K, V> right) {
            this.left = left;
            this.right = right;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean matches(K key, V value, long timestamp, States store) {
            return left.matches(key, value, timestamp, store) || right.matches(key, value, timestamp, store);
        }
    }
}
