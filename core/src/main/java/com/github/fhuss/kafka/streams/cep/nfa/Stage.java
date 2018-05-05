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

import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.pattern.MatcherContext;
import com.github.fhuss.kafka.streams.cep.state.ReadOnlySharedVersionBuffer;
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
 *
 * The implementation is based on the paper "Efficient Pattern Matching over Event Streams".
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class Stage<K, V> implements Serializable, Comparable<Stage<K, V>> {

    private int id;
    private String name;
    private StateType type;
    private long windowMs = -1;
    private List<StateAggregator<K, V, Object>> aggregates;
    private List<Edge<K, V>> edges;


    /**
     * Dummy constructor required by Kryo.
     */
    public Stage() {}


    /**
     * Creates a new {@link Stage} instance.
     *
     * @param id    the identifier of the stage.
     * @param name  the name of the stage.
     * @param type  the type of the stage.
     */
    public Stage(final int id, final String name, final StateType type) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.edges = new ArrayList<>();
    }

    public void setAggregates(final List<StateAggregator<K, V, Object>> aggregator) {
        this.aggregates = aggregator;
    }

    List<StateAggregator<K, V, Object>> getAggregates() {
        return this.aggregates;
    }

    public Set<String> getStates() {
        if (aggregates == null) return Collections.emptySet();
        return getAggregates()
                .stream()
                .map(StateAggregator::getName)
                .collect(Collectors.toSet());
    }

    public long getWindowMs() {
        return this.windowMs;
    }

    public Stage<K, V> setWindow(final long windowMs) {
        this.windowMs = windowMs;
        return this;
    }

    public Stage<K, V> addEdge(final Edge<K, V> edge) {
        this.edges.add(edge);
        return this;
    }

    public int getId() {
        return id;
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

    public boolean isEpsilonStage() {
        return this.edges.size() == 1 && this.edges.get(0).getOperation().equals(EdgeOperation.PROCEED);
    }

    public Stage<K, V> getTargetByOperation(final EdgeOperation op) {
        Stage<K, V> target = null;
        for(Edge<K, V> edge : this.edges)
            if( edge.getOperation().equals(op))
                target = edge.target;
        return target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Stage<?, ?> that = (Stage<?, ?>) o;
        return Objects.equals(this.name, that.name) &&
                this.type == that.type &&
                this.id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type);
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
        public Edge(final EdgeOperation operation, final Matcher<K, V> predicate, final Stage<K, V> target) {
            if( predicate == null ) throw new IllegalArgumentException("predicate cannot be null");
            if( operation == null ) throw new IllegalArgumentException("operation cannot be null");
            this.operation = operation;
            this.predicate = predicate;
            this.target = target;
        }

        public EdgeOperation getOperation() {
            return operation;
        }

        boolean accept(final MatcherContext<K, V> context) {
            return predicate.accept(context);
        }

        public Stage<K, V> getTarget() {
            return target;
        }

        public boolean is(final EdgeOperation o) {
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
        sb.append("id='").append(id).append('\'');
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", edges={");
        for (int i = 0; i < edges.size(); i++) {
            sb.append("Edge{");
            sb.append("operation=").append(edges.get(i).operation);
            if (edges.get(i).target != null) {
                sb.append(", target={");
                sb.append("name='").append(edges.get(i).target.name).append("'");
                sb.append("}");
            }
            sb.append("}");
            if (i + 1 < edges.size()) {
                sb.append(", ");
            }
        }
        sb.append("}");
        sb.append('}');
        return sb.toString();
    }

    public enum StateType {
        BEGIN, NORMAL, FINAL
    }

    public static <K, V> Stage<K, V> newEpsilonState(final Stage<K, V> current, final Stage<K, V> target) {
        Stage<K, V> newStage = new Stage<>(current.id, current.getName(), current.getType());
        newStage.addEdge(new Stage.Edge<>(EdgeOperation.PROCEED, new Matcher.TruePredicate<>(), target));
        return newStage;
    }
}
