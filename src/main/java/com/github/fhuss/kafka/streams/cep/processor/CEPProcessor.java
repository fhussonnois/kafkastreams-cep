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
import com.github.fhuss.kafka.streams.cep.state.internal.TopicAndPartition;
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

    private NFA<K, V> nfa;

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
    private void ensureInitStates(List<Stage<K, V>> stages) {
        if( this.nfa == null ) {
            final TopicAndPartition tp = getTopicAndPartition();
            LOG.info("Initializing NFA for {}", tp);

            NFAStates<K, V> nfaState = nfaStore.find(tp);

            if (nfaState != null) {
                LOG.info("Loading existing nfa states for {}, latest offset {}", tp, nfaState.getLatestOffset());
                this.nfa = new NFA<>(aggregatesStore, bufferStore, nfaState.getRuns(), nfaState.getComputationStages());
                this.highwatermark = nfaState.getLatestOffset();
            } else {
                this.nfa = new NFA<>(aggregatesStore, bufferStore, stages);
            }
        }
    }

    private TopicAndPartition getTopicAndPartition() {
        return new TopicAndPartition(context.topic(), context.partition());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(K key, V value) {
        ensureInitStates(this.stages);
        if(value != null && checkHighWaterMarkAndUpdate()) {
            Event<K, V> event = new Event<>(key, value, context.timestamp(), context.topic(), context.partition(), context.offset());
            List<Sequence<K, V>> sequences = this.nfa.matchPattern(event);
            persistNFA();
            sequences.forEach(seq -> this.context.forward(null, seq));
        }
    }

    private void persistNFA() {
        TopicAndPartition key = getTopicAndPartition();
        this.nfaStore.put(key, new NFAStates<>(this.nfa.getComputationStages(), this.nfa.getRuns(), context.offset() + 1));
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
