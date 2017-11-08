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
import com.github.fhuss.kafka.streams.cep.nfa.NFA;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.KVSharedVersionedBuffer;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.processor.internal.NFAStateValue;
import com.github.fhuss.kafka.streams.cep.state.StateStoreProvider;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.processor.internal.TopicAndPartition;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CEPProcessor<K, V> implements Processor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPProcessor.class);

    private List<Stage<K, V>> stages;

    private ProcessorContext context;

    private String queryName;

    private boolean inMemory;

    private NFA<K, V> nfa;

    private final String bufferStateStoreName;

    private final String nfaStateStoreName;

    private Long highwatermark = -1L;

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param queryName
     * @param pattern
     */
    public CEPProcessor(String queryName, Pattern<K, V> pattern) {
        this(queryName, pattern, false);
    }

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param pattern
     */
    public CEPProcessor(String queryName, Pattern<K, V> pattern, boolean inMemory) {
        StagesFactory<K, V> fact = new StagesFactory<>();
        this.stages = fact.make(pattern);
        this.inMemory = inMemory;
        this.queryName = queryName.toLowerCase().replace("\\s+", "");

        this.bufferStateStoreName = StateStoreProvider.getEventBufferStoreName(this.queryName);
        this.nfaStateStoreName    = StateStoreProvider.getNFAStoreName(this.queryName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
    }

    private NFA<K, V> initializeIfNotAndGet(List<Stage<K, V>> stages) {
        if( this.nfa == null ) {
            LOG.info("Initializing NFA for topic={}, partition={}", context.topic(), context.partition());
            KVSharedVersionedBuffer.Factory<K, V> bufferFactory = KVSharedVersionedBuffer.getFactory();
            KeyValueStore<TopicAndPartition, NFAStateValue<K, V>>  nfaStore = getNFAStore();
            TopicAndPartition tp = new TopicAndPartition(context.topic(), context.partition());
            NFAStateValue<K, V> nfaState = nfaStore.get(tp);
            KVSharedVersionedBuffer<K, V> buffer = bufferFactory.make(context, bufferStateStoreName);
            if (nfaState != null) {
                LOG.info("Loading existing nfa states for {}, latest offset {}", tp, nfaState.getLatestOffset());
                this.nfa = new NFA<>(new StateStoreProvider(queryName, context), buffer, nfaState.getRuns(), nfaState.getComputationStages());
                this.highwatermark = nfaState.getLatestOffset();
            } else {
                this.nfa = new NFA<>(new StateStoreProvider(queryName, context), buffer, stages);
            }
        }
        return this.nfa;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(K key, V value) {
        initializeIfNotAndGet(this.stages);
        if(value != null && checkHighWaterMarkAndUpdate()) {
            Event<K, V> event = new Event<>(key, value, context.timestamp(), context.topic(), context.partition(), context.offset());
            List<Sequence<K, V>> sequences = this.nfa.matchPattern(event);
            KeyValueStore<TopicAndPartition, NFAStateValue<K, V>> store = getNFAStore();
            store.put(new TopicAndPartition(context.topic(), context.partition()), new NFAStateValue<>(this.nfa.getComputationStages(), this.nfa.getRuns(), context.offset() + 1));
            sequences.forEach(seq -> this.context.forward(null, seq));
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

    @SuppressWarnings("unchecked")
    private KeyValueStore<TopicAndPartition, NFAStateValue<K, V>> getNFAStore() {
        return (KeyValueStore<TopicAndPartition, NFAStateValue<K, V>>) this.context.getStateStore(nfaStateStoreName);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
