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
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.state.StateStoreProvider;
import com.github.fhuss.kafka.streams.cep.state.States;
import com.github.fhuss.kafka.streams.cep.state.ValueStore;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.KVSharedVersionedBuffer;
import com.github.fhuss.kafka.streams.cep.pattern.StateAggregator;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Non-determinism Finite Automaton.
 *
 * @param <K>
 * @param <V>
 */
public class NFA<K, V> implements Serializable {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(NFA.class);

    private StateStoreProvider storeProvider;

    private KVSharedVersionedBuffer<K, V> sharedVersionedBuffer;

    private Queue<ComputationStage<K, V>> computationStages;

    private AtomicLong runs;

    /**
     * Creates a new {@link NFA} instance.
     */
    public NFA(StateStoreProvider storeProvider, KVSharedVersionedBuffer<K, V> buffer, Collection<Stage<K, V>> stages) {
        this(storeProvider, buffer, 1L, initComputationStates(stages));
    }

    /**
     * Creates a new {@link NFA} instance.
     */
    public NFA(StateStoreProvider storeProvider, KVSharedVersionedBuffer<K, V> buffer, Long runs, Queue<ComputationStage<K, V>> computationStages) {
        this.storeProvider = storeProvider;
        this.sharedVersionedBuffer = buffer;
        this.computationStages = computationStages;
        this.runs = new AtomicLong(runs);
    }

    public long getRuns() {
        return runs.get();
    }

    private static <K, V> Queue<ComputationStage<K, V>> initComputationStates(Collection<Stage<K, V>> stages) {
        Queue<ComputationStage<K, V>> q = new LinkedBlockingQueue<>();
        stages.forEach(s -> {
            if (s.isBeginState())
                q.add(new ComputationStageBuilder().setStage(s).setVersion(new DeweyVersion(1)).setSequence(1L).build());
        });
        return q;
    }

    public Queue<ComputationStage<K, V>> getComputationStages() {
        return computationStages;
    }

    /**
     * Process the message with the given key and value.
     *
     * @param event the event to match.
     */
    public List<Sequence<K, V>> matchPattern(Event<K, V> event) {

        int numberOfStateToProcess = computationStages.size();

        List<ComputationStage<K, V>> finalStates = new LinkedList<>();
        while(numberOfStateToProcess-- > 0) {
            ComputationStage<K, V> computationStage = computationStages.poll();
            Collection<ComputationStage<K, V>> states = matchPattern(new ComputationContext<>(event, computationStage));
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
        if( !ctx.getComputationStage().isBeginState() && ctx.getComputationStage().isOutOfWindow(ctx.getEvent().timestamp) )
            return nextComputationStages;

        nextComputationStages = evaluate(ctx, ctx.getComputationStage().getStage(), null);

        // Begin state should always be re-add to allow multiple runs.
        if(ctx.getComputationStage().isBeginState() && !ctx.getComputationStage().isForwarding()) {
            DeweyVersion version = ctx.getComputationStage().getVersion();
            DeweyVersion newVersion = (nextComputationStages.isEmpty()) ? version : version.addRun();
            ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<K, V>()
                    .setStage(ctx.getComputationStage().getStage())
                    .setVersion(newVersion)
                    .setSequence(runs.incrementAndGet());
            nextComputationStages.add(builder.build());
        }

        return nextComputationStages;
    }

    private Collection<ComputationStage<K, V>> evaluate(ComputationContext<K, V> ctx, Stage<K, V> currentStage, Stage<K, V> previousStage) {
        ComputationStage<K, V> computationStage = ctx.computationStage;
        final long sequenceID = computationStage.getSequence();
        final Event<K, V> previousEvent = computationStage.getEvent();
        final DeweyVersion version      = computationStage.getVersion();

        List<Stage.Edge<K, V>> matchedEdges = matchEdgesAndGet(ctx.event, sequenceID, currentStage);

        Collection<ComputationStage<K, V>> nextComputationStages = new ArrayList<>();
        final boolean isBranching = isBranching(matchedEdges);
        Event<K, V> currentEvent = ctx.getEvent();

        long startTime = ctx.getFirstPatternTimestamp();
        boolean consumed = false;
        boolean ignored  = false;

        for(Stage.Edge<K, V> e : matchedEdges) {
            Stage<K, V> epsilonStage = Stage.newEpsilonState(currentStage, e.getTarget());
            LOG.debug("[{}]{} - {}", sequenceID, e.getOperation(), currentEvent);
            switch (e.getOperation()) {
                case PROCEED:
                    ComputationContext<K, V> nextContext = ctx;
                    // Checks whether epsilon operation is forwarding to a new stage and doesn't result from new branch.
                    if ( ! e.getTarget().equals(currentStage) && !ctx.computationStage.isBranching() ) {
                        ComputationStage<K, V> newStage = ctx.computationStage.setVersion(ctx.computationStage.getVersion().addStage());
                        nextContext = new ComputationContext<>(ctx.event, newStage);
                    }
                    nextComputationStages.addAll(evaluate(nextContext, e.getTarget(), currentStage));
                    break;
                case TAKE :
                    // The event has matched the current event and not the next one.
                    if( ! isBranching ) {
                        // Re-add the current stage to NFA
                        ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<K, V>()
                                .setStage(Stage.newEpsilonState(currentStage, currentStage))
                                .setVersion(version)
                                .setEvent(currentEvent)
                                .setTimestamp(startTime)
                                .setSequence(sequenceID);
                        nextComputationStages.add(builder.build());
                        // Add the current event to buffer using current version path.
                        putToSharedBuffer(currentStage, previousStage, previousEvent, currentEvent, version);
                    } else {
                        putToSharedBuffer(currentStage, previousStage, previousEvent, currentEvent, version.addRun());
                    }
                    // Else the event is handle by PROCEED edge.
                    consumed = true;
                    break;
                case BEGIN :
                    // Add the current event to buffer using current version path
                    putToSharedBuffer(currentStage, previousStage, previousEvent, currentEvent, version);
                    // Compute the next stage in NFA
                    ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<K, V>()
                            .setStage(epsilonStage)
                            .setVersion(version)
                            .setEvent(currentEvent)
                            .setTimestamp(startTime)
                            .setSequence(sequenceID);
                    nextComputationStages.add(builder.build());
                    consumed = true;
                    break;
                case IGNORE:
                    // Re-add the current stage to NFA
                    if(!isBranching) nextComputationStages.add(computationStage);
                    ignored = true;
                    break;
            }
        }

        if(isBranching) {
            LOG.debug("new branch from event {}", currentEvent);
            long newSequence = runs.incrementAndGet();
            Event<K, V> latestMatchEvent = ignored ? previousEvent : currentEvent;
            ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<K, V>()
                    .setStage(Stage.newEpsilonState(previousStage, currentStage))
                    .setVersion(version.addRun())
                    .setEvent(latestMatchEvent)
                    .setTimestamp(startTime)
                    .setSequence(newSequence)
                    .setBranching(true);
            nextComputationStages.add(builder.build());
            currentStage.getAggregates().forEach(agg -> storeProvider.getValueStore(agg.getName(), sequenceID).branch(newSequence));

            sharedVersionedBuffer.branch(previousStage, previousEvent, version);
        }

        if( consumed ) evaluateAggregates(currentStage.getAggregates(), sequenceID, ctx.event.key, ctx.event.value);
        return nextComputationStages;
    }

    private void putToSharedBuffer(Stage<K, V> currentStage, Stage<K, V> previousStage, Event<K, V> previousEvent, Event<K, V> currentEvent, DeweyVersion nextVersion) {
        if( previousStage != null)
            sharedVersionedBuffer.put(currentStage, currentEvent, previousStage, previousEvent, nextVersion);
        else
            sharedVersionedBuffer.put(currentStage, currentEvent, nextVersion);
    }

    @SuppressWarnings("unchecked")
    private void evaluateAggregates(List<StateAggregator<K, V, Object>> aggregates, long sequence, K key, V value) {
        aggregates.forEach(agg -> {
            ValueStore store = storeProvider.getValueStore(agg.getName(), sequence);
            if( store == null) throw new IllegalStateException("State store is not defined: " + agg.getName());
            store.set(agg.getAggregate().aggregate(key, value, store.get()));
        });
    }

    private List<Stage.Edge<K, V>> matchEdgesAndGet(Event<K, V> event, long sequence, Stage<K, V> currentStage) {
        States store = new States(storeProvider, sequence);
        return currentStage.getEdges()
                .stream()
                .filter(e -> e.matches(event.key, event.value, event.timestamp, store))
                .collect(Collectors.toList());
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
         * The current state to compute.
         */
        private final ComputationStage<K, V> computationStage;

        private final Event<K, V> event;

        /**
         * Creates a new {@link ComputationContext} instance.
         */
        private ComputationContext(Event<K, V> event, ComputationStage<K, V> computationStage) {
            this.event = event;
            this.computationStage = computationStage;
        }

        public ComputationStage<K, V> getComputationStage() {
            return computationStage;
        }

        public long getFirstPatternTimestamp() {
            return computationStage.isBeginState() ? event.timestamp : computationStage.getTimestamp();
        }

        public Event<K, V> getEvent( ) {
            return event;
        }
    }
}