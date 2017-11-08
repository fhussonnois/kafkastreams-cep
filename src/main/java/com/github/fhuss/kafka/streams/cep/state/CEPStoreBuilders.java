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
package com.github.fhuss.kafka.streams.cep.state;

import com.github.fhuss.kafka.streams.cep.nfa.ComputationStageSerDe;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.TimedKeyValueSerDes;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.serde.KryoSerDe;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class CEPStoreBuilders<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPStoreBuilders.class);

    private final KryoSerDe kryoSerDe;
    private final StagesFactory<K, V> stagesFactory;

    /**
     * Creates a new {@link CEPStoreBuilders} instance.
     */
    public CEPStoreBuilders() {
        this.kryoSerDe = new KryoSerDe();
        this.stagesFactory = new StagesFactory<>();
    }

    public Collection<StoreBuilder> get(final String queryName,
                                               final Pattern<K, V> pattern,
                                               final Serde<K> keySerde,
                                               final Serde<V> valueSerde) {

        return get(queryName, pattern, keySerde, valueSerde, false);
    }

    public Topology addStateStores(final Topology topology,
                               final String processorName,
                               final String queryName,
                               final Pattern<K, V> pattern,
                               final Serde<K> keySerde,
                               final Serde<V> valueSerde) {

        Collection<StoreBuilder> builders = get(queryName, pattern, keySerde, valueSerde);
        for(StoreBuilder storeBuilder : builders) {
            LOG.info("State store registered with name {}", storeBuilder.name());
            topology.addStateStore(storeBuilder, processorName);
        }

        return topology;
    }

    public Collection<StoreBuilder> get(final String queryName,
                                                      final Pattern<K, V> pattern,
                                                      final Serde<K> keySerde,
                                                      final Serde<V> valueSerde,
                                                      final boolean inMemory) {
        List<Stage<K, V>> stages = stagesFactory.make(pattern);

        Collection<StoreBuilder> builders = new LinkedList<>();

        for(String state : getDefinedStateNames(stages)) {
            final String storeName = StateStoreProvider.getStateStoreName(queryName, state);
            builders.add(Stores.keyValueStoreBuilder(getStateStoreSupplier(storeName, inMemory), kryoSerDe, kryoSerDe));
        }

        String bufferStateStoreName = StateStoreProvider.getEventBufferStoreName(queryName);
        String nfaStateStoreName    = StateStoreProvider.getNFAStoreName(queryName);

        TimedKeyValueSerDes<K, V> timedKeyValueSerDes = new TimedKeyValueSerDes<>(keySerde, valueSerde);

        KeyValueBytesStoreSupplier bufferStateStoreSupplier = getStateStoreSupplier(bufferStateStoreName, inMemory);
        builders.add(Stores.keyValueStoreBuilder(bufferStateStoreSupplier, kryoSerDe, Serdes.serdeFrom(timedKeyValueSerDes, timedKeyValueSerDes)));

        NFAStateValueSerDe valueSerDe = new NFAStateValueSerDe<>(new ComputationStageSerDe<>(stages, keySerde, valueSerde));
        builders.add(Stores.keyValueStoreBuilder(getStateStoreSupplier(nfaStateStoreName, inMemory), kryoSerDe, Serdes.serdeFrom(valueSerDe, valueSerDe)));

        return builders;
    }

    private KeyValueBytesStoreSupplier getStateStoreSupplier(final String name, final boolean isMemory) {
        return isMemory ? Stores.inMemoryKeyValueStore(name) : Stores.persistentKeyValueStore(name);
    }

    private Set<String> getDefinedStateNames(List<Stage<K, V>> stages) {
        return stages
                .stream()
                .flatMap(s -> s.getStates().stream())
                .distinct()
                .collect(Collectors.toSet());
    }
}
