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
import java.util.Stack;
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

            Collection<ComputationState<K, V>> states = matchPattern(this.context, key, value, timestamp, computationState);
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
                .filter(c -> ! isForwardingToFinalState(c))
                .collect(Collectors.toList());
    }

    private List<ComputationState<K, V>> getAllFinalStates(Collection<ComputationState<K, V>> states) {
        return states
                .stream()
                .filter(this::isForwardingToFinalState)
                .collect(Collectors.toList());
    }

    private boolean isForwardingToFinalState(ComputationState<K, V> c) {
        List<State.Edge<K, V>> edges = c.getState().getEdges();
        return ( edges.size() > 0
                && edges.get(0).is(EdgeOperation.PROCEED)
                && edges.get(0).getTarget().isFinalState());
    }

    private Collection<ComputationState<K, V>> matchPattern(ProcessorContext context, K key, V value, long timestamp, ComputationState<K, V> computationState) {
        Collection<ComputationState<K, V>> nextComputationStates = new ArrayList<>();

        final Event<K, V> previousEvent = computationState.getEvent();
        final State<K, V> state = computationState.getState();
        final DeweyVersion version = computationState.getVersion();
        final boolean isBeginState = state.isBeginState();

        if( !isBeginState && computationState.isOutOfWindow(timestamp) )
            return nextComputationStates;

        final long nextTimestamp = isBeginState ? timestamp : computationState.getTimestamp();

        Stack<State<K, V>> states = new Stack<>();
        states.push(state);

        State<K, V> previousState = null;
        while( ! states.isEmpty() ) {
            State<K, V> currentState = states.pop();

            List<State.Edge<K, V>> matchedEdges = currentState.getEdges()
                    .stream()
                    .filter(e -> e.matches(key, value, timestamp))
                    .collect(Collectors.toList());

            List<EdgeOperation> matchedOperations = matchedEdges
                    .stream()
                    .map(State.Edge::getOperation)
                    .collect(Collectors.toList());

            final boolean isBranching = isBranching(matchedOperations);
            Event<K, V> currentEvent = newEvent(context, key, value);
            for(State.Edge<K, V> e : matchedEdges) {
                State<K, V> epsilonState = newEpsilonState(currentState, e.getTarget());
                switch (e.getOperation()) {
                    case PROCEED:
                        previousState = currentState;
                        states.push(e.getTarget());
                        break;
                    case TAKE :
                        if( ! isBranching ) {
                            nextComputationStates.add(computationState);
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
                        nextComputationStates.add(new ComputationState<>(epsilonState, version.addStage(), currentEvent, nextTimestamp));
                        break;
                    case IGNORE:
                        if(!isBranching)
                            nextComputationStates.add(computationState);
                        break;
                }
            }

            if(isBranching) {
                State<K, V> epsilonState = newEpsilonState(currentState, currentState);
                nextComputationStates.add(new ComputationState<>(epsilonState, version.addRun(), currentEvent, nextTimestamp));
            }
        }

        if( isBeginState ) {
            nextComputationStates.add(new ComputationState<>(state, version.addRun()));
        }

        return nextComputationStates;
    }

    private State<K, V> newEpsilonState(State<K, V> current, State<K, V> target) {
        State<K, V> newState = new State<>(current.getName(), current.getType());
        newState.addEdge(new State.Edge<>(EdgeOperation.PROCEED, (k, v,t, s) -> true, target));

        return newState;
    }

    private boolean isBranching(Collection<EdgeOperation> matchedOperations) {
        return matchedOperations.containsAll(Arrays.asList(EdgeOperation.PROCEED, EdgeOperation.TAKE) )  // allowed with multiple match
                || matchedOperations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.TAKE) ) // allowed by skip-till-any-match
                || matchedOperations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.BEGIN) ) // allowed by skip-till-any-match
                || matchedOperations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.PROCEED) ); //allowed by skip-till-next-match or skip-till-any-match
    }

    private Event<K, V> newEvent(ProcessorContext context, K key, V value) {
        return new Event<>(key, value, context.timestamp(), context.topic(), context.partition(), context.offset());
    }
}



