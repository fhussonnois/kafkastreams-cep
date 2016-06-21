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
package com.github.fhuz.kafka.streams.cep;

import com.github.fhuz.kafka.streams.cep.nfa.NFA;
import com.github.fhuz.kafka.streams.cep.nfa.Stage;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.KVSharedVersionedBuffer;
import com.github.fhuz.kafka.streams.cep.pattern.NFAFactory;
import com.github.fhuz.kafka.streams.cep.pattern.Pattern;
import com.github.fhuz.kafka.streams.cep.serde.KryoSerDe;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class CEPProcessor<K, V> implements Processor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPProcessor.class);

    public static final String DEFAULT_STATE_STORE = "_cep_sharedbuffer_events";

    private Pattern<K, V> pattern;

    private ProcessorContext context;

    private NFA<K, V> nfa;

    /**
     * Creates a new {@link CEPProcessor} instance.
     * @param pattern
     */
    public CEPProcessor(Pattern<K, V> pattern) {
        this.pattern = pattern;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        KVSharedVersionedBuffer<K, V> buffer = KVSharedVersionedBuffer.<K, V>getFactory().make(context);

        NFAFactory<K, V> fact = new NFAFactory<>();
        List<Stage<K, V>> stages = fact.make(pattern);
        this.nfa = new NFA<>(context, buffer, stages);
    }

    public static StateStoreSupplier getEventsStore(boolean isMemory) {
        KryoSerDe serde = new KryoSerDe();
        Stores.KeyValueFactory factory = Stores.create(DEFAULT_STATE_STORE)
                .withKeys(serde)
                .withValues(serde);
        return isMemory ? factory.inMemory().build() : factory.persistent().build();
    }

    @Override
    public void process(K key, V value) {
        LOG.info("process {},{}", key, value);
        List<Sequence<K, V>> sequences = this.nfa.matchPattern(key, value, context.timestamp());
        sequences.forEach( seq -> this.context.forward(null, seq));
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
