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
import com.github.fhuz.kafka.streams.cep.Sequence;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.KVSharedVersionedBuffer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
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

    private Collection<Stage<K, V>> stages;

    private Queue<ComputationStage<K, V>> computationStages;

    private transient ProcessorContext context;

    /**
     * Creates a new {@link NFA} instance.
     */
    public NFA(ProcessorContext context, KVSharedVersionedBuffer<K, V> buffer, Collection<Stage<K, V>> stages) {
        this.stages = stages;
        this.context = context;
        this.computationStages = new LinkedBlockingQueue<>();
        this.sharedVersionedBuffer = buffer;
        initComputationStates(stages);
    }

    private void initComputationStates(Collection<Stage<K, V>> stages) {
        stages.forEach(s -> {
            if (s.isBeginState())
                computationStages.add(new ComputationStage<>(s, new DeweyVersion(1)));
        });
    }

    public Collection<Stage<K, V>> getStages() {
        return stages;
    }

    /**
     * Process the message with the given key and value.
     *
     * @param key the key for the message
     * @param value the value for the message
     * @param timestamp The timestamp of the current event.
     */
    public List<Sequence<K, V>> matchPattern(K key, V value, long timestamp) {

        int numberOfStateToProcess = computationStages.size();

        List<ComputationStage<K, V>> finalStates = new LinkedList<>();
        while(numberOfStateToProcess-- > 0) {
            ComputationStage<K, V> computationStage = computationStages.poll();
            Collection<ComputationStage<K, V>> states = matchPattern(new ComputationContext<>(this.context, key, value, timestamp, computationStage));
            if( states.isEmpty() )
                removePattern(computationStage);
            else
                finalStates.addAll(getAllFinalStates(states));
            computationStages.addAll(getAllNonFinalStates(states));
        }
        return matchConstruction(finalStates);
    }

    private List<Sequence<K, V>> matchConstruction(Collection<ComputationStage<K, V>> states) {
        return  states.stream()
                .map(c -> sharedVersionedBuffer.remove(c.getStage(), c.getEvent(), c.getVersion()))
                .collect(Collectors.toList());
    }

    private void removePattern(ComputationStage<K, V> computationStage) {
        sharedVersionedBuffer.remove(
                computationStage.getStage(),
                computationStage.getEvent(),
                computationStage.getVersion()
        );
    }

    private List<ComputationStage<K, V>> getAllNonFinalStates(Collection<ComputationStage<K, V>> states) {
        return states
                .stream()
                .filter(c -> !c.isForwardingToFinalState())
                .collect(Collectors.toList());
    }

    private List<ComputationStage<K, V>> getAllFinalStates(Collection<ComputationStage<K, V>> states) {
        return states
                .stream()
                .filter(ComputationStage::isForwardingToFinalState)
                .collect(Collectors.toList());
    }

    private Collection<ComputationStage<K, V>> matchPattern(ComputationContext<K, V> ctx) {
        Collection<ComputationStage<K, V>> nextComputationStages = new ArrayList<>();

        // Checks the time window of the current state.
        if( !ctx.getComputationStage().isBeginState() && ctx.getComputationStage().isOutOfWindow(ctx.getTimestamp()) )
            return nextComputationStages;

        nextComputationStages = evaluate(ctx, ctx.getComputationStage().getStage(), null);

        // Begin state should always be re-add to allow multiple runs.
        if(ctx.getComputationStage().isBeginState() && !ctx.getComputationStage().isForwarding()) {
            DeweyVersion version = ctx.getComputationStage().getVersion();
            DeweyVersion newVersion = (nextComputationStages.isEmpty()) ? version : version.addRun();
            nextComputationStages.add(new ComputationStage<>(ctx.getComputationStage().getStage(), newVersion));
        }

        return nextComputationStages;
    }

    private Collection<ComputationStage<K, V>> evaluate(ComputationContext<K, V> ctx, Stage<K, V> currentStage, Stage<K, V> previousStage) {
        ComputationStage<K, V> computationStage = ctx.computationStage;
        final Event<K, V> previousEvent = computationStage.getEvent();
        final DeweyVersion version      = computationStage.getVersion();

        List<Stage.Edge<K, V>> matchedEdges = matchEdgesAndGet(ctx.key, ctx.value, ctx.timestamp, currentStage);

        Collection<ComputationStage<K, V>> nextComputationStages = new ArrayList<>();
        final boolean isBranching = isBranching(matchedEdges);
        Event<K, V> currentEvent = ctx.getEvent();
        for(Stage.Edge<K, V> e : matchedEdges) {
            Stage<K, V> epsilonStage = newEpsilonState(currentStage, e.getTarget());
            switch (e.getOperation()) {
                case PROCEED:
                    nextComputationStages.addAll(evaluate(ctx, e.getTarget(), currentStage));
                    break;
                case TAKE :
                    if( ! isBranching ) {
                        nextComputationStages.add(computationStage.setEvent(currentEvent));
                        sharedVersionedBuffer.put(currentStage, currentEvent, previousStage, previousEvent, version);
                    } else {
                        sharedVersionedBuffer.put(currentStage, currentEvent, version.addRun());
                    }
                    if( computationStage.isBranching() ) sharedVersionedBuffer.branch(previousStage, previousEvent, version);
                    break;
                case BEGIN :
                    if( previousStage != null)
                        sharedVersionedBuffer.put(currentStage, currentEvent, previousStage, previousEvent, version);
                    else
                        sharedVersionedBuffer.put(currentStage, currentEvent, version);
                    nextComputationStages.add(new ComputationStage<>(epsilonStage, version.addStage(), currentEvent, ctx.getFirstPatternTimestamp()));
                    if( computationStage.isBranching() ) sharedVersionedBuffer.branch(previousStage, previousEvent, version);
                    break;
                case IGNORE:
                    if(!isBranching)
                        nextComputationStages.add(computationStage);
                    break;
            }
        }
        if(isBranching) {
            Stage<K, V> epsilonStage = newEpsilonState(previousStage, currentStage);
            ComputationStage<K, V> newStage = new ComputationStage<>(epsilonStage, version.addRun(), computationStage.getEvent(), ctx.getFirstPatternTimestamp());
            newStage.setBranching(true);
            nextComputationStages.add(newStage);
        }
        return nextComputationStages;
    }

    private List<Stage.Edge<K, V>> matchEdgesAndGet(K key, V value, long timestamp, Stage<K, V> currentStage) {
        final StateStore stateStore = currentStage.getState() != null ? context.getStateStore(currentStage.getState()) : null;
        return currentStage.getEdges()
                .stream()
                .filter(e -> e.matches(key, value, timestamp, stateStore))
                .collect(Collectors.toList());
    }

    private Stage<K, V> newEpsilonState(Stage<K, V> current, Stage<K, V> target) {
        Stage<K, V> newStage = new Stage<>(current.getName(), current.getType());
        newStage.addEdge(new Stage.Edge<>(EdgeOperation.PROCEED, (k, v, t, s) -> true, target));

        return newStage;
    }

    private boolean isBranching(Collection<Stage.Edge<K, V>> edges) {
        List<EdgeOperation> matchedOperations = edges
                .stream()
                .map(Stage.Edge::getOperation)
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
        private final ComputationStage<K, V> computationStage;

        /**
         * Creates a new {@link ComputationContext} instance.
         */
        private ComputationContext(ProcessorContext context, K key, V value, long timestamp, ComputationStage<K, V> computationStage) {
            this.context = context;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.computationStage = computationStage;
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

        public ComputationStage<K, V> getComputationStage() {
            return computationStage;
        }

        public long getFirstPatternTimestamp() {
            return computationStage.isBeginState() ? timestamp : computationStage.getTimestamp();
        }

        public Event<K, V> getEvent( ) {
            return new Event<>(key, value, context.timestamp(), context.topic(), context.partition(), context.offset());
        }
    }
}