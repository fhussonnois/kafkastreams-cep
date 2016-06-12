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
package com.github.fhuz.kafka.streams.cep.nfa;

import com.github.fhuz.kafka.streams.cep.Event;
import com.github.fhuz.kafka.streams.cep.State;
import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.KVSharedVersionedBuffer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Non-determinism Finite Automaton.
 *
 * @param <K>
 * @param <V>
 */
public class NFA<K, V> implements Serializable {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(NFA.class);

    private KVSharedVersionedBuffer<K, V> sharedVersionedBuffer;

    private Collection<State<K, V>> states;

    private Queue<ComputationState<K, V>> computationStates;

    private transient ProcessorContext context;

    /**
     * Creates a new {@link NFA} instance.
     */
    public NFA(ProcessorContext context, KVSharedVersionedBuffer<K, V> buffer, Collection<State<K, V>> states) {
        this.states = states;
        this.context = context;
        this.computationStates = new LinkedBlockingQueue<>();
        this.sharedVersionedBuffer = buffer;
        initComputationStates(states);
    }

    private void initComputationStates(Collection<State<K, V>> states) {
        states.forEach(s -> {
            if (s.isBeginState())
                computationStates.add(new ComputationState<>(s, new DeweyVersion(1)));
        });
    }

    public Collection<State<K, V>> getStates() {
        return states;
    }

    /**
     * Process the message with the given key and value.
     *
     * @param key the key for the message
     * @param value the value for the message
     * @param timestamp The timestamp of the current event.
     */
    public List<Sequence<K, V>> matchPattern(K key, V value, long timestamp) {

        int numberOfStateToProcess = computationStates.size();
        List<Sequence<K, V>> sequences = new LinkedList<>();
        while(numberOfStateToProcess-- > 0) {
            ComputationState<K, V> computationState = computationStates.poll();

            Collection<ComputationState<K, V>> states = matchPattern(new ComputationContext<K, V>(this.context, key, value, timestamp, computationState));
            if( states.isEmpty() )
                removePattern(computationState);
            else
                sequences.addAll(matchConstruction(states));
            computationStates.addAll(getAllNonFinalStates(states));
        }
        return sequences;
    }

    private List<Sequence<K, V>> matchConstruction(Collection<ComputationState<K, V>> states) {
        return getAllFinalStates(states)
                .stream()
                .map(c -> sharedVersionedBuffer.get(c.getState(), c.getEvent(), c.getVersion()))
                .collect(Collectors.toList());
    }

    private void removePattern(ComputationState<K, V> computationState) {
        sharedVersionedBuffer.remove(
                computationState.getState(),
                computationState.getEvent(),
                computationState.getVersion()
        );
    }

    private List<ComputationState<K, V>> getAllNonFinalStates(Collection<ComputationState<K, V>> states) {
        return states
                .stream()
                .filter(c -> !c.isForwardingToFinalState())
                .collect(Collectors.toList());
    }

    private List<ComputationState<K, V>> getAllFinalStates(Collection<ComputationState<K, V>> states) {
        return states
                .stream()
                .filter(ComputationState::isForwardingToFinalState)
                .collect(Collectors.toList());
    }

    private Collection<ComputationState<K, V>> matchPattern(ComputationContext<K, V> ctx) {
        Collection<ComputationState<K, V>> nextComputationStates = new ArrayList<>();

        // Checks the time window of the current state.
        if( !ctx.getComputationState().isBeginState() && ctx.getComputationState().isOutOfWindow(ctx.getTimestamp()) )
            return nextComputationStates;

        nextComputationStates = evaluate(ctx, ctx.getComputationState().getState(), null);

        // Begin state should always be re-add to allow multiple runs.
        if(ctx.getComputationState().isBeginState() && !ctx.getComputationState().isForwarding()) {
            DeweyVersion version = ctx.getComputationState().getVersion();
            DeweyVersion newVersion = (nextComputationStates.isEmpty()) ? version : version.addRun();
            nextComputationStates.add(new ComputationState<>(ctx.getComputationState().getState(), newVersion));
        }

        return nextComputationStates;
    }

    private Collection<ComputationState<K, V>> evaluate(ComputationContext<K, V> ctx, State<K, V> currentState, State<K, V> previousState) {
        ComputationState<K, V> computationState = ctx.computationState;
        final Event<K, V> previousEvent = computationState.getEvent();
        final DeweyVersion version      = computationState.getVersion();

        List<State.Edge<K, V>> matchedEdges = matchEdgesAndGet(ctx.key, ctx.value, ctx.timestamp, currentState);

        Collection<ComputationState<K, V>> nextComputationStates = new ArrayList<>();
        final boolean isBranching = isBranching(matchedEdges);
        Event<K, V> currentEvent = ctx.getEvent();
        for(State.Edge<K, V> e : matchedEdges) {
            State<K, V> epsilonState = newEpsilonState(currentState, e.getTarget());
            switch (e.getOperation()) {
                case PROCEED:
                    nextComputationStates.addAll(evaluate(ctx, e.getTarget(), currentState));
                    break;
                case TAKE :
                    if( ! isBranching ) {
                        nextComputationStates.add(computationState.setEvent(currentEvent));
                        sharedVersionedBuffer.put(currentState, currentEvent, previousState, previousEvent, version);
                    } else {
                        sharedVersionedBuffer.put(currentState, currentEvent, version.addRun());
                    }
                    break;
                case BEGIN :
                    if( previousState != null)
                        sharedVersionedBuffer.put(currentState, currentEvent, previousState, previousEvent, version);
                    else
                        sharedVersionedBuffer.put(currentState, currentEvent, version);
                    nextComputationStates.add(new ComputationState<>(epsilonState, version.addStage(), currentEvent, ctx.getFirstPatternTimestamp()));
                    break;
                case IGNORE:
                    if(!isBranching)
                        nextComputationStates.add(computationState);
                    break;
            }
        }

        if(isBranching) {
            State<K, V> epsilonState = newEpsilonState(currentState, currentState);
            nextComputationStates.add(new ComputationState<>(epsilonState, version.addRun(), currentEvent, ctx.getFirstPatternTimestamp()));
        }
        return nextComputationStates;
    }

    private List<State.Edge<K, V>> matchEdgesAndGet(K key, V value, long timestamp, State<K, V> currentState) {
        return currentState.getEdges()
                .stream()
                .filter(e -> e.matches(key, value, timestamp))
                .collect(Collectors.toList());
    }

    private State<K, V> newEpsilonState(State<K, V> current, State<K, V> target) {
        State<K, V> newState = new State<>(current.getName(), current.getType());
        newState.addEdge(new State.Edge<>(EdgeOperation.PROCEED, (k, v,t, s) -> true, target));

        return newState;
    }

    private boolean isBranching(Collection<State.Edge<K, V>> edges) {
        List<EdgeOperation> matchedOperations = edges
                .stream()
                .map(State.Edge::getOperation)
                .collect(Collectors.toList());
        return matchedOperations.containsAll(Arrays.asList(EdgeOperation.PROCEED, EdgeOperation.TAKE) )  // allowed with multiple match
                || matchedOperations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.TAKE) ) // allowed by skip-till-any-match
                || matchedOperations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.BEGIN) ) // allowed by skip-till-any-match
                || matchedOperations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.PROCEED) ); //allowed by skip-till-next-match or skip-till-any-match
    }

    /**
     * Class to wrap all data required to evaluate a state against an event.
     */
    private static class ComputationContext<K, V> {
        /**
         * The current processor context.
         */
        private final ProcessorContext context;
        /**
         * The current event key.
         */
        private final K key;
        /**
         * The current event value.
         */
        private final V value;
        /**
         * The current event timestamp.
         */
        private final long timestamp;
        /**
         * The current state to compute.
         */
        private final ComputationState<K, V> computationState;

        /**
         * Creates a new {@link ComputationContext} instance.
         */
        private ComputationContext(ProcessorContext context, K key, V value, long timestamp, ComputationState<K, V> computationState) {
            this.context = context;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.computationState = computationState;
        }

        public ProcessorContext getContext() {
            return context;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public ComputationState<K, V> getComputationState() {
            return computationState;
        }

        public long getFirstPatternTimestamp() {
            return computationState.isBeginState() ? timestamp : computationState.getTimestamp();
        }

        public Event<K, V> getEvent( ) {
            return new Event<>(key, value, context.timestamp(), context.topic(), context.partition(), context.offset());
        }
    }
}