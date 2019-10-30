/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.processor;

import com.github.fhuss.kafka.streams.cep.core.Event;
import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.nfa.NFA;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stages;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.core.state.internal.NFAStates;
import com.github.fhuss.kafka.streams.cep.core.state.internal.Runned;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStateStore;
import com.github.fhuss.kafka.streams.cep.state.NFAStateStore;
import com.github.fhuss.kafka.streams.cep.state.QueryStores;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStateStore;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The default {@link Processor} implementation used to run a user-defined {@link Pattern}.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class CEPProcessor<K, V> implements Processor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPProcessor.class);

    private Stages<K, V> stages;

    private ProcessorContext context;

    private String queryName;

    private SharedVersionedBufferStateStore<K, V> bufferStore;

    private AggregatesStateStore<K> aggregatesStore;

    private NFAStateStore<K, V> nfaStore;

    private NFAStates<K, V> currentNFAState;

    private StreamsMetricsImpl metrics;

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param queryName the complex pattern query name.
     * @param pattern   the complex pattern.
     */
    public CEPProcessor(final String queryName,
                        final Pattern<K, V> pattern) {
        this(queryName, new StagesFactory<K, V>().make(pattern));
    }

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param queryName the complex pattern query name.
     * @param stages   the complex pattern.
     */
    CEPProcessor(final String queryName,
                 final Stages<K, V> stages) {
        this.stages = stages;
        this.queryName = queryName.toLowerCase().replace("\\s+", "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;

        metrics = (StreamsMetricsImpl) context.metrics();

        final String nfaStateStoreName = QueryStores.getQueryNFAStoreName(queryName);
        nfaStore = (NFAStateStore<K, V>) context.getStateStore(nfaStateStoreName);
        validateStateStore(nfaStore, nfaStateStoreName);

        final String eventBufferStoreName = QueryStores.getQueryEventBufferStoreName(queryName);
        bufferStore = (SharedVersionedBufferStateStore<K, V>) context.getStateStore(eventBufferStoreName);
        validateStateStore(bufferStore, eventBufferStoreName);

        final String aggregateStatesStoreName = QueryStores.getQueryAggregateStatesStoreName(queryName);
        aggregatesStore = (AggregatesStateStore<K>) context.getStateStore(aggregateStatesStoreName);
        validateStateStore(aggregatesStore, aggregateStatesStoreName);
    }

    private static void validateStateStore(final StateStore stateStore, final String storeName) {
        if (stateStore == null) {
            throw new IllegalStateException("Cannot find store with name " + storeName);
        }
    }

    private NFA<K, V> loadNFA(final Stages<K, V> stages, final K key) {
        final Runned<K> runned = new Runned<>(key);
        currentNFAState = nfaStore.find(runned);
        NFA<K, V> nfa;
        if (currentNFAState != null) {
            LOG.debug("Recovering existing nfa states for {}, latest offset {}",
                runned,
                currentNFAState.getLatestOffsets());
            nfa = new NFA<>(
                aggregatesStore,
                bufferStore,
                stages.getDefinedStates(),
                currentNFAState.getComputationStages(),
                currentNFAState.getRuns());
        } else {
            nfa = NFA.build(stages, aggregatesStore, bufferStore);
            currentNFAState = new NFAStates<>(nfa.getComputationStages(), nfa.getRuns());
        }
        return nfa;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(final K key, final V value) {
        List<Sequence<K, V>> sequences = match(key, value);
        sequences.forEach(seq -> context().forward(key, seq));
    }

    public List<Sequence<K, V>> match(final K key, final V value) {
        // we ignore record if either key or value is null
        if (key == null || value == null) {
            LOG.warn(
                "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                key, value, context().topic(), context().partition(), context().offset()
            );
            metrics.skippedRecordsSensor().record();
            return Collections.emptyList();
        }

        final NFA<K, V> nfa = loadNFA(stages, key);

        if (!checkLastObservedOffset(key, value)) {
            return Collections.emptyList();
        }

        Event<K, V> event = buildEventFor(key, value);
        List<Sequence<K, V>> sequences = nfa.matchPattern(event);

        Map<String, Long> latestOffsets = currentNFAState.getLatestOffsets();
        latestOffsets.put(context().topic(), context().offset() + 1);
        currentNFAState = new NFAStates<>(nfa.getComputationStages(), nfa.getRuns(), latestOffsets);
        nfaStore.put(new Runned<>(key), currentNFAState);

        return sequences;
    }

    private Event<K, V> buildEventFor(final K key, final V value) {
        return new Event<>(
            key,
            value,
            context().timestamp(),
            context().topic(),
            context().partition(),
            context().offset());
    }

    private boolean checkLastObservedOffset(final K key, final V value) {
        Long latestOffset = currentNFAState.getLatestOffsetForTopic(context().topic());
        if (latestOffset == null) latestOffset = -1L;
        if (context().offset() < latestOffset) {
            LOG.warn(
                "Skipping record due to offset prior to the current high-water mark({}). " +
                "key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                latestOffset,
                key,
                value,
                context().topic(),
                context().partition(),
                context().offset()
            );
            return false;
        }
        return true;
    }

    ProcessorContext context() {
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }
}
