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
import com.github.fhuz.kafka.streams.cep.nfa.buffer.impl.KVSharedVersionedBuffer;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.impl.TimedKeyValue;
import com.github.fhuz.kafka.streams.cep.nfa.buffer.impl.TimedKeyValueSerDes;
import com.github.fhuz.kafka.streams.cep.pattern.StatesFactory;
import com.github.fhuz.kafka.streams.cep.pattern.Pattern;
import com.github.fhuz.kafka.streams.cep.serde.KryoSerDe;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class CEPProcessor<K, V> implements Processor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPProcessor.class);

    private static final String BUFFER_EVENT_STORE = "_cep_buffer_events";

    private List<Stage<K, V>> stages;

    private ProcessorContext context;

    private boolean inMemory;

    private NFA<K, V> nfa;

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param pattern
     */
    public CEPProcessor(Pattern<K, V> pattern) {
        this(pattern, false);
    }

    /**
     * Creates a new {@link CEPProcessor} instance.
     *
     * @param pattern
     */
    public CEPProcessor(Pattern<K, V> pattern, boolean inMemory) {
        StatesFactory<K, V> fact = new StatesFactory<>();
        this.stages = fact.make(pattern);
        this.inMemory = inMemory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;

        KryoSerDe kryoSerDe = new KryoSerDe();
        Set<StateStoreSupplier> stateStoreSuppliers = getDefinedStateNames(stages)
                .map(s -> getStateStoreSupplier(s, kryoSerDe, kryoSerDe, inMemory))
                .collect(Collectors.toSet());

        TimedKeyValueSerDes<K, V> timedKeyValueSerDes = new TimedKeyValueSerDes(this.context.keySerde(), this.context.valueSerde());
        Serde<TimedKeyValue<K, V>> valueSerde = Serdes.serdeFrom(timedKeyValueSerDes, timedKeyValueSerDes);
        stateStoreSuppliers.add(getStateStoreSupplier(BUFFER_EVENT_STORE, kryoSerDe, valueSerde, inMemory));

        initializeStateStores(stateStoreSuppliers);
        initializeNFA(stages);
    }

    private Stream<String> getDefinedStateNames(List<Stage<K, V>> stages) {
        return stages
                .stream()
                .flatMap(s -> s.getStates().stream())
                .distinct();
    }

    private void initializeNFA(List<Stage<K, V>> stages) {
        KVSharedVersionedBuffer.Factory<K, V> bufferFactory = KVSharedVersionedBuffer.getFactory();
        this.nfa = new NFA<>(context, bufferFactory.make(context, BUFFER_EVENT_STORE), stages);
    }

    private void initializeStateStores(Collection<StateStoreSupplier> suppliers) {
        for (StateStoreSupplier stateStoreSupplier : suppliers) {
            StateStore store = stateStoreSupplier.get();
            store.init(this.context, store);
            LOG.info("State store registered with name {}", store.name());
        }
    }

    public StateStoreSupplier getStateStoreSupplier(String name, Serde keys, Serde values, boolean isMemory) {
        Stores.KeyValueFactory factory = Stores.create(name)
                .withKeys(keys)
                .withValues(values);
        return isMemory ? factory.inMemory().build() : factory.persistent().build();
    }

    @Override
    public void process(K key, V value) {
        if(value != null) {
            List<Sequence<K, V>> sequences = this.nfa.matchPattern(key, value, context.timestamp());
            sequences.forEach(seq -> this.context.forward(null, seq));
        }
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
