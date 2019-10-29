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
package com.github.fhuss.kafka.streams.cep.core.nfa;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.pattern.MatcherContext;
import com.github.fhuss.kafka.streams.cep.core.pattern.StateAggregator;
import com.github.fhuss.kafka.streams.cep.core.state.AggregatesStore;
import com.github.fhuss.kafka.streams.cep.core.state.ReadOnlySharedVersionBuffer;
import com.github.fhuss.kafka.streams.cep.core.state.SharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.core.state.States;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Aggregate;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Aggregated;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Matched;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.github.fhuss.kafka.streams.cep.core.nfa.EdgeOperation.SKIP_PROCEED;

/**
 * Non-determinism Finite Automaton.
 *
 * The implementation is based on the paper "Efficient Pattern Matching over Event Streams".
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class NFA<K, V> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NFA.class);

    private static final long INITIAL_RUNS = 1L;

    private SharedVersionedBufferStore<K, V> sharedVersionedBuffer;

    private Queue<ComputationStage<K, V>> computationStages;

    private Set<String> aggregatesName;

    private final AggregatesStore<K> aggregatesStore;

    private AtomicLong runs;

    public static <K, V> NFA<K, V> build(final Stages<K, V> stages,
                                         final AggregatesStore<K> aggregatesStore,
                                         final SharedVersionedBufferStore<K, V> sharedVersionedBuffer) {
        LinkedBlockingQueue<ComputationStage<K, V>> queue = new LinkedBlockingQueue<>();
        queue.add(stages.initialComputationStage());
        return new NFA<>(aggregatesStore, sharedVersionedBuffer, stages.getDefinedStates(), queue);
    }

    /**
     * Creates a new {@link NFA} instance.
     *
     * @param aggregatesStore       the instance of AggregateStore to used.
     * @param sharedVersionedBuffer the instance of sharedVersionedBuffer to used.
     * @param aggregatesName        the list of aggregate names.
     * @param computationStages     the queue of stages to compute.
     */
    public NFA(final AggregatesStore<K> aggregatesStore,
                final SharedVersionedBufferStore<K, V> sharedVersionedBuffer,
                final Set<String> aggregatesName,
                final Queue<ComputationStage<K, V>> computationStages) {
        this(aggregatesStore, sharedVersionedBuffer, aggregatesName, computationStages, INITIAL_RUNS);
    }

    /**
     * Creates a new {@link NFA} instance.
     *
     * @param aggregatesStore       the instance of AggregateStore to used.
     * @param sharedVersionedBuffer the instance of sharedVersionedBuffer to used.
     * @param aggregatesName        the list of aggregate names.
     * @param computationStages     the queue of stages to compute.
     * @param runs                  the current runs
     */
    public NFA(final AggregatesStore<K> aggregatesStore,
               final SharedVersionedBufferStore<K, V> sharedVersionedBuffer,
               final Set<String> aggregatesName,
               final Queue<ComputationStage<K, V>> computationStages, final long runs) {
        Objects.requireNonNull(aggregatesStore, "aggregateStore cannot be null");
        Objects.requireNonNull(sharedVersionedBuffer, "sharedVersionedBuffer cannot be null");
        Objects.requireNonNull(computationStages, "computationStages cannot be null");
        Objects.requireNonNull(aggregatesName, "aggregatesName cannot be null");
        this.aggregatesName = aggregatesName;
        this.sharedVersionedBuffer = sharedVersionedBuffer;
        this.computationStages = computationStages;
        this.runs = new AtomicLong(runs);
        this.aggregatesStore = aggregatesStore;
    }

    public long getRuns() {
        return runs.get();
    }

    public Queue<ComputationStage<K, V>> getComputationStages() {
        return computationStages;
    }

    /**
     * Process the message with the given key and value.
     *
     * @param   event the event to match.
     * @return  sequences of matching events.
     */
    public List<Sequence<K, V>> matchPattern(final Event<K, V> event) {

        int numberOfStateToProcess = computationStages.size();

        List<ComputationStage<K, V>> finalStates = new LinkedList<>();
        while(numberOfStateToProcess-- > 0) {
            ComputationStage<K, V> computationStage = computationStages.poll();
            Collection<ComputationStage<K, V>> states = matchPattern(new ComputationContext<>(event, computationStage));
            if (states.isEmpty())
                removePattern(computationStage);
            else
                finalStates.addAll(getAllFinalStates(states));
            computationStages.addAll(getAllNonFinalStates(states));
        }
        return matchConstruction(finalStates);
    }

    private List<Sequence<K, V>> matchConstruction(final Collection<ComputationStage<K, V>> states) {
        return  states.stream()
                .map(c -> {
                    Matched matched = Matched.from(c.getStage(), c.getLastEvent());
                    return sharedVersionedBuffer.remove(matched, c.getVersion());
                })
                .collect(Collectors.toList());
    }

    private void removePattern(ComputationStage<K, V> computationStage) {
        final Matched matched = Matched.from(computationStage.getStage(), computationStage.getLastEvent());
        sharedVersionedBuffer.remove(matched, computationStage.getVersion());
    }

    private List<ComputationStage<K, V>> getAllNonFinalStates(final Collection<ComputationStage<K, V>> states) {
        return states
                .stream()
                .filter(c -> !c.isForwardingToFinalState())
                .collect(Collectors.toList());
    }

    private List<ComputationStage<K, V>> getAllFinalStates(final  Collection<ComputationStage<K, V>> states) {
        return states
                .stream()
                .filter(ComputationStage::isForwardingToFinalState)
                .collect(Collectors.toList());
    }

    private Collection<ComputationStage<K, V>> matchPattern(ComputationContext<K, V> ctx) {
        Collection<ComputationStage<K, V>> nextComputationStages = new ArrayList<>();

        // Checks the time window of the current state.
        if( !ctx.getComputationStage().isBeginState() && ctx.getComputationStage().isOutOfWindow(ctx.getEvent().timestamp()) )
            return nextComputationStages;

        return evaluate(ctx, ctx.getComputationStage().getStage(), null);

    }

    private Collection<ComputationStage<K, V>> evaluate(final ComputationContext<K, V> ctx,
                                                        final Stage<K, V> currentStage,
                                                        final Stage<K, V> previousStage) {
        ComputationStage<K, V> computationStage = ctx.computationStage;
        final long sequenceId = computationStage.getSequence();
        final Event<K, V> previousEvent = computationStage.getLastEvent();
        final DeweyVersion version      = computationStage.getVersion();

        List<Stage.Edge<K, V>> matchedEdges = matchEdgesAndGet(previousEvent, ctx.event, version, sequenceId, previousStage, currentStage);

        Collection<ComputationStage<K, V>> nextComputationStages = new ArrayList<>();

        List<EdgeOperation> operations = matchedEdges
                .stream()
                .map(Stage.Edge::getOperation)
                .collect(Collectors.toList());

        final boolean isBranching = isBranching(operations);
        Event<K, V> currentEvent = ctx.getEvent();

        long startTime = ctx.getFirstPatternTimestamp();
        boolean consumed = false;
        boolean proceed = false;

        final boolean ignored  = operations.contains(EdgeOperation.IGNORE);

        for (Stage.Edge<K, V> edge : matchedEdges) {
            final EdgeOperation operation = edge.getOperation();
            LOG.debug("Matching stage with: name = {}, run = {}, version = {}, operation = {}, event = {}",
                    currentStage.getName(), sequenceId, version, operation, currentEvent);
            final ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<>();
            switch (operation) {
                case PROCEED :
                case SKIP_PROCEED:
                    ComputationContext<K, V> nextContext = ctx;
                    // Checks whether epsilon operation is forwarding to a new stage and doesn't result from new branch.
                    if (isForwardingToNextStage(currentStage, computationStage, edge)) {
                        ComputationStage<K, V> newStage = computationStage.setVersion(version.addStage());
                        nextContext = new ComputationContext<>(ctx.event, newStage);
                    }

                    Stage<K, V> previous = operation.equals(SKIP_PROCEED) ? previousStage : currentStage;
                    Collection<ComputationStage<K, V>> stages = evaluate(nextContext, edge.getTarget(), previous);
                    nextComputationStages.addAll(stages);
                    if (!stages.isEmpty()) {
                        proceed = true;
                    }
                    break;
                case TAKE :
                    // The event has matched the current event and not the next one.
                    // Re-add the current stage to NFA
                    builder.setStage(Stage.newEpsilonState(currentStage, currentStage))
                            .setVersion(version)
                            .setEvent(currentEvent)
                            .setTimestamp(startTime)
                            .setSequence(sequenceId);
                    nextComputationStages.add(builder.build());
                    if (!isBranching || ignored) {
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
                    Stage<K, V> epsilonStage = Stage.newEpsilonState(currentStage, edge.getTarget());

                    builder.setStage(epsilonStage)
                            .setVersion(version)
                            .setEvent(currentEvent)
                            .setTimestamp(startTime)
                            .setSequence(sequenceId);

                    nextComputationStages.add(builder.build());
                    consumed = true;
                    break;
                case IGNORE:
                    // Re-add the current stage to NFA if the event does not match.
                    if (!isBranching) {
                        builder
                             .setSequence(computationStage.getSequence())
                             .setEvent(computationStage.getLastEvent())
                             .setTimestamp(computationStage.getTimestamp())
                             .setStage(computationStage.getStage())
                             .setVersion(computationStage.getVersion())
                             .setIgnore(true);
                        nextComputationStages.add(builder.build());
                    }
                    //
                    break;
            }
        }

        if (isBranching) {
            if (consumed) {
                long newSequence = runs.incrementAndGet();
                Event<K, V> lastEvent = ignored ? previousEvent : currentEvent;
                final Stage<K, V> stage = Stage.newEpsilonState(previousStage, currentStage);
                final DeweyVersion nextVersion = previousStage.isBeginState() ? version.addRun(2) : version.addRun();
                LOG.debug("Branching new run with id = {}, version = {}, stage = {}, event ={}",
                        newSequence, nextVersion, stage.getName(), currentEvent);
                ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<K, V>()
                        .setStage(stage)
                        .setVersion(nextVersion)
                        .setEvent(lastEvent)
                        .setTimestamp(startTime)
                        .setSequence(newSequence)
                        .setBranching(true);
                nextComputationStages.add(builder.build());

                this.aggregatesName.forEach(agg -> {
                    Aggregated<K> aggregated = new Aggregated<>(currentEvent.key(), new Aggregate(agg, sequenceId));
                    aggregatesStore.branch(aggregated, newSequence);
                });

                if (!previousStage.isBeginState()) {
                    sharedVersionedBuffer.branch(previousStage, previousEvent, version);
                }
            } else if (!proceed){
                nextComputationStages.add(ctx.getComputationStage());
            }
        }

        if (consumed) {
            evaluateAggregates(currentStage.getAggregates(), sequenceId, ctx.event.key(), ctx.event.value());
        }

        // Begin state should always be re-add to allow multiple runs.
        if(ctx.getComputationStage().isBeginState() && !ctx.getComputationStage().isForwarding()) {
            if (consumed) {
                long newSequence = this.runs.incrementAndGet();
                DeweyVersion newVersion = (nextComputationStages.isEmpty()) ? version : version.addRun();
                LOG.debug("Branching new run with id = {}, version = {}, stage = {}, event ={}",
                        newSequence, newVersion, currentStage.getName(), currentEvent);
                ComputationStageBuilder<K, V> builder = new ComputationStageBuilder<K, V>()
                        .setStage(ctx.getComputationStage().getStage())
                        .setVersion(newVersion)
                        .setSequence(newSequence);
                nextComputationStages.add(builder.build());
            } else {
                nextComputationStages.add(ctx.getComputationStage());
            }
        }

        return nextComputationStages;
    }

    private boolean isForwardingToNextStage(final Stage<K, V> currentStage,
                                            final ComputationStage<K, V> computationStage,
                                            final Stage.Edge<K, V> edge) {
        return ! (edge.getTarget().getName().equals(currentStage.getName())) &&
                !computationStage.isBranching() &&
                !computationStage.isIgnored();
    }

    private void putToSharedBuffer(final Stage<K, V> currentStage,
                                   final Stage<K, V> previousStage,
                                   final Event<K, V> previousEvent,
                                   final Event<K, V> currentEvent,
                                   final DeweyVersion nextVersion) {
        if (previousStage != null)
            sharedVersionedBuffer.put(currentStage, currentEvent, previousStage, previousEvent, nextVersion);
        else
            sharedVersionedBuffer.put(currentStage, currentEvent, nextVersion);
    }

    private void evaluateAggregates(List<StateAggregator<K, V, Object>> aggregates, long sequence, K key, V value) {
        aggregates.forEach(agg -> {
            Aggregated<K> aggregated = new Aggregated<>(key, new Aggregate(agg.getName(), sequence));
            Object o = aggregatesStore.find(aggregated);
            Object newValue = agg.getAggregate().aggregate(key, value, o);
            aggregatesStore.put(aggregated, newValue);
        });
    }

    private List<Stage.Edge<K, V>> matchEdgesAndGet(final Event<K, V> previousEvent,
                                                    final Event<K, V> currentEvent,
                                                    final DeweyVersion version,
                                                    final long sequence,
                                                    final Stage<K, V> previousStage,
                                                    final Stage<K, V> currentStage) {

        States<K> states = new States<>(aggregatesStore, currentEvent.key(), sequence);
        ReadOnlySharedVersionBuffer<K, V> readOnlySharedVersionBuffer = new ReadOnlySharedVersionBuffer<>(sharedVersionedBuffer);
        return currentStage.getEdges()
                .stream()
                .filter(e -> e.accept(new MatcherContext<>(readOnlySharedVersionBuffer, version, previousStage, currentStage, previousEvent, currentEvent, states)))
                .collect(Collectors.toList());
    }

    /**
     * A run can be split when the current event actually matches two edges. A split may occur even with strict or non strict contiguity.
     * @param operations list of edge operations that are matched the current event.
     *
     * @return <code>true</code>
     */
    private boolean isBranching(final Collection<EdgeOperation> operations) {
        return operations.containsAll(Arrays.asList(EdgeOperation.PROCEED, EdgeOperation.TAKE) )  // allowed with multiple match
                || operations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.TAKE) ) // allowed by skip-till-any-match
                || operations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.BEGIN) ) // allowed by skip-till-any-match
                || operations.containsAll(Arrays.asList(EdgeOperation.IGNORE, EdgeOperation.PROCEED) ); //allowed by skip-till-next-match or skip-till-any-match
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
        private ComputationContext(final Event<K, V> event, final ComputationStage<K, V> computationStage) {
            this.event = event;
            this.computationStage = computationStage;
        }

        public ComputationStage<K, V> getComputationStage() {
            return computationStage;
        }

        public long getFirstPatternTimestamp() {
            return computationStage.isBeginState() ? event.timestamp() : computationStage.getTimestamp();
        }

        public Event<K, V> getEvent( ) {
            return event;
        }
    }
}