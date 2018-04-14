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
package com.github.fhuss.kafka.streams.cep.processor;

import com.github.fhuss.kafka.streams.cep.Event;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStore;
import com.github.fhuss.kafka.streams.cep.nfa.NFA;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.state.NFAStore;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStore;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.state.internal.NFAStates;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.state.internal.Runned;
import com.github.fhuss.kafka.streams.cep.state.QueryStores;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CEPProcessor<K, V> implements Processor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPProcessor.class);

    private List<Stage<K, V>> stages;


    private ProcessorContext context;

    private String queryName;

    private Long highwatermark = -1L;

    private SharedVersionedBufferStore<K, V> bufferStore;

    private AggregatesStore<K> aggregatesStore;

    private NFAStore<K, V> nfaStore;

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param pattern
     */
    public CEPProcessor(final String queryName,
                        final Pattern<K, V> pattern) {
        StagesFactory<K, V> fact = new StagesFactory<>();
        this.stages = fact.make(pattern);
        this.queryName = queryName.toLowerCase().replace("\\s+", "");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;

        this.nfaStore = (NFAStore<K, V>)
                context.getStateStore(QueryStores.getQueryNFAStoreName(queryName));

        this.bufferStore = (SharedVersionedBufferStore<K, V> )
                context.getStateStore(QueryStores.getQueryEventBufferStoreName(queryName));

        this.aggregatesStore = (AggregatesStore<K>)
                context.getStateStore(QueryStores.getQueryAggregateStatesStoreName(queryName));
    }

    @SuppressWarnings("unchecked")
    private NFA<K, V> loadNFA(List<Stage<K, V>> stages, K key) {
        final Runned<K> runned = getRunned(key);

        NFAStates<K, V> nfaState = nfaStore.find(runned);
        NFA<K, V> nfa;
        if (nfaState != null) {
            LOG.info("Loading existing nfa states for {}, latest offset {}", runned, nfaState.getLatestOffset());
            nfa = new NFA<>(aggregatesStore, bufferStore, nfaState.getRuns(), nfaState.getComputationStages());
            this.highwatermark = nfaState.getLatestOffset();
        } else {
            nfa = new NFA<>(aggregatesStore, bufferStore, stages);
        }
        return nfa;
    }

    private Runned<K> getRunned(final K key) {
        return new Runned<>(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(K key, V value) {
        // If the key or value is null we don't need to proceed
        if (key == null || value == null) {
            return;
        }
        final NFA<K, V> nfa = loadNFA(this.stages, key);
        if (checkHighWaterMarkAndUpdate()) {
            Event<K, V> event = new Event<>(key, value, context.timestamp(), context.topic(), context.partition(), context.offset());
            List<Sequence<K, V>> sequences = nfa.matchPattern(event);
            NFAStates<K, V> state = new NFAStates<>(nfa.getComputationStages(), nfa.getRuns(), context.offset() + 1);
            this.nfaStore.put(getRunned(key), state);
            sequences.forEach(seq -> this.context.forward(key, seq));
        }
    }

    private boolean checkHighWaterMarkAndUpdate() {
        if (this.context.offset() < this.highwatermark) {
            LOG.warn("Offset({}) is prior to the current high-water mark({})", this.context.offset(), this.highwatermark);
            return false;
        }
        this.highwatermark = this.context.offset();
        return true;
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
