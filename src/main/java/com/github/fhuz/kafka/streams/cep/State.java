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
package com.github.fhuz.kafka.streams.cep;

import com.github.fhuz.kafka.streams.cep.nfa.EdgeOperation;
import com.github.fhuz.kafka.streams.cep.pattern.Matcher;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 */
public class State<K, V> implements Serializable, Comparable<State<K, V>> {

    private String name;
    private StateType type;
    private long windowMs = -1;

    /**
     * Dummy constructor required by Kryo.
     */
    public State() {}

    private List<Edge<K, V>> edges;

    /**
     * Creates a new {@link State} instance.
     * @param name
     * @param type
     */
    public State(String name, StateType type) {
        this.name = name;
        this.type = type;
        this.edges = new ArrayList<>();
    }

    public long getWindowMs() {
        return this.windowMs;
    }

    public State<K, V> setWindow(long windowMs) {
        this.windowMs = windowMs;
        return this;
    }

    public State<K, V> addEdge(Edge<K, V> edge) {
        this.edges.add(edge);
        return this;
    }

    public List<Edge<K, V>> getEdges() {
        return edges;
    }

    public boolean isBeginState() {
        return type.equals(StateType.BEGIN);
    }

    public boolean isFinalState() {
        return type.equals(StateType.FINAL);
    }

    public String getName() {
        return name;
    }

    public StateType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        State<?, ?> state = (State<?, ?>) o;
        return Objects.equals(name, state.name) &&
                type == state.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public int compareTo(State<K, V> that) {
        return this.name.compareTo(that.name);
    }

    public static class Edge<K, V> implements Serializable {
        private EdgeOperation operation;
        private Matcher<K, V> predicate;
        private State<K, V> target;

        /**
         * Creates a new {@łink Edge} instance.
         *
         * @param operation the edge operation.
         * @param predicate the predicate to apply.
         * @param target the state to move forward if predicate is true.
         */
        public Edge(EdgeOperation operation, Matcher<K, V> predicate, State<K, V> target) {
            if( predicate == null ) throw new IllegalArgumentException("predicate cannot be null");
            if( operation == null ) throw new IllegalArgumentException("operation cannot be null");
            this.operation = operation;
            this.predicate = predicate;
            this.target = target;
        }

        public EdgeOperation getOperation() {
            return operation;
        }

        public boolean matches(K key, V value, long timestamp) {
            return predicate.matches(key, value, timestamp, null);
        }

        public State<K, V> getTarget() {
            return target;
        }

        public boolean is(EdgeOperation o) {
            return this.operation.equals(o);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Edge{");
            sb.append("operation=").append(operation);
            sb.append(", predicate=").append(predicate);
            sb.append(", target=").append(target);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("State{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", edges=").append(edges);
        sb.append('}');
        return sb.toString();
    }

    public enum StateType {
        BEGIN, NORMAL, FINAL;
    }
}
