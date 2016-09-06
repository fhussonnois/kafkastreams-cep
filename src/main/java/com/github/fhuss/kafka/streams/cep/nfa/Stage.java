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
package com.github.fhuss.kafka.streams.cep.nfa;

import com.github.fhuss.kafka.streams.cep.state.States;
import com.github.fhuss.kafka.streams.cep.pattern.Matcher;
import com.github.fhuss.kafka.streams.cep.pattern.StateAggregator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf
 */
public class Stage<K, V> implements Serializable, Comparable<Stage<K, V>> {

    private String name;
    private StateType type;
    private long windowMs = -1;
    private List<StateAggregator<K, V, Object>> aggregates;


    static <K, V> Stage<K, V> newEpsilonState(Stage<K, V> current, Stage<K, V> target) {
        Stage<K, V> newStage = new Stage<>(current.getName(), current.getType());
        newStage.addEdge(new Stage.Edge<>(EdgeOperation.PROCEED, (k, v, t, s) -> true, target));
        return newStage;
    }

    /**
     * Dummy constructor required by Kryo.
     */
    public Stage() {}

    private List<Edge<K, V>> edges;

    /**
     * Creates a new {@link Stage} instance.
     * @param name
     * @param type
     */
    public Stage(String name, StateType type) {
        this.name = name;
        this.type = type;
        this.edges = new ArrayList<>();
    }

    public void setAggregates(List<StateAggregator<K, V, Object>> aggregator) {
        this.aggregates = aggregator;
    }

    List<StateAggregator<K, V, Object>> getAggregates() {
        return this.aggregates;
    }

    public Set<String> getStates() {
        if( aggregates == null) return Collections.EMPTY_SET;
        return getAggregates()
                .stream()
                .map(StateAggregator::getName)
                .collect(Collectors.toSet());
    }

    long getWindowMs() {
        return this.windowMs;
    }

    public Stage<K, V> setWindow(long windowMs) {
        this.windowMs = windowMs;
        return this;
    }

    public Stage<K, V> addEdge(Edge<K, V> edge) {
        this.edges.add(edge);
        return this;
    }

    List<Edge<K, V>> getEdges() {
        return edges;
    }

    boolean isBeginState() {
        return type.equals(StateType.BEGIN);
    }

    boolean isFinalState() {
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
        Stage<?, ?> stage = (Stage<?, ?>) o;
        return Objects.equals(name, stage.name) && type == stage.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    boolean isEpsilonStage() {
        return this.edges.size() == 1 && this.edges.get(0).getOperation().equals(EdgeOperation.PROCEED);
    }

    Stage<K, V> getTargetByOperation(EdgeOperation op) {
        Stage<K, V> target = null;
        for(Edge<K, V> edge : this.edges)
            if( edge.getOperation().equals(op))
                target = edge.target;
        return target;
    }

    @Override
    public int compareTo(Stage<K, V> that) {
        return this.name.compareTo(that.name);
    }

    public static class Edge<K, V> implements Serializable {
        private EdgeOperation operation;
        private Matcher<K, V> predicate;
        private Stage<K, V> target;

        /**
         * Creates a new {@link Edge} instance.
         *
         * @param operation the edge operation.
         * @param predicate the predicate to apply.
         * @param target the state to move forward if predicate is true.
         */
        public Edge(EdgeOperation operation, Matcher<K, V> predicate, Stage<K, V> target) {
            if( predicate == null ) throw new IllegalArgumentException("predicate cannot be null");
            if( operation == null ) throw new IllegalArgumentException("operation cannot be null");
            this.operation = operation;
            this.predicate = predicate;
            this.target = target;
        }

        EdgeOperation getOperation() {
            return operation;
        }

        boolean matches(K key, V value, long timestamp, States store) {
            return predicate.matches(key, value, timestamp, store);
        }

        Stage<K, V> getTarget() {
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
        final StringBuilder sb = new StringBuilder("Stage{");
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
