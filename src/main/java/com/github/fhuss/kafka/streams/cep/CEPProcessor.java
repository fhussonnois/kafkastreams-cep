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
package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.nfa.ComputationStage;
import com.github.fhuss.kafka.streams.cep.nfa.NFA;
import com.github.fhuss.kafka.streams.cep.nfa.Stage;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.KVSharedVersionedBuffer;
import com.github.fhuss.kafka.streams.cep.nfa.buffer.impl.TimedKeyValueSerDes;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.serde.KryoSerDe;
import com.github.fhuss.kafka.streams.cep.state.StateStoreProvider;
import com.github.fhuss.kafka.streams.cep.nfa.ComputationStageSerDe;
import com.github.fhuss.kafka.streams.cep.pattern.StagesFactory;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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

        KryoSerDe kryoSerDe = new KryoSerDe();
        Set<StateStoreSupplier> stateStoreSuppliers = getDefinedStateNames(stages)
                .map(s -> getStateStoreSupplier(StateStoreProvider.getStateStoreName(queryName, s), kryoSerDe, kryoSerDe, inMemory))
                .collect(Collectors.toSet());

        Serde<?> keySerde = this.context.keySerde();
        Serde<?> valSerde = this.context.valueSerde();

        TimedKeyValueSerDes<K, V> timedKeyValueSerDes = new TimedKeyValueSerDes(keySerde, valSerde);
        stateStoreSuppliers.add(getStateStoreSupplier(bufferStateStoreName, kryoSerDe,
                Serdes.serdeFrom(timedKeyValueSerDes, timedKeyValueSerDes), inMemory));

        NFASTateValueSerDe valueSerDe = new NFASTateValueSerDe(new ComputationStageSerDe(stages, keySerde, valSerde));
        stateStoreSuppliers.add(getStateStoreSupplier(nfaStateStoreName, kryoSerDe,
                Serdes.serdeFrom(valueSerDe, valueSerDe), inMemory));

        initializeStateStores(stateStoreSuppliers);
    }

    private Stream<String> getDefinedStateNames(List<Stage<K, V>> stages) {
        return stages
                .stream()
                .flatMap(s -> s.getStates().stream())
                .distinct();
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
                LOG.info("Loading existing nfa states for {}, latest offset {}", tp, nfaState.latestOffset);
                this.nfa = new NFA<>(new StateStoreProvider(queryName, context), buffer, nfaState.runs, nfaState.computationStages);
                this.highwatermark = nfaState.latestOffset;
            } else {
                this.nfa = new NFA<>(new StateStoreProvider(queryName, context), buffer, stages);
            }
        }
        return this.nfa;
    }

    private void initializeStateStores(Collection<StateStoreSupplier> suppliers) {
        for (StateStoreSupplier stateStoreSupplier : suppliers) {
            StateStore store = stateStoreSupplier.get();
            store.init(this.context, store);
            LOG.info("State store registered with name {}", store.name());
        }
    }

    private StateStoreSupplier getStateStoreSupplier(String name, Serde keys, Serde values, boolean isMemory) {
        Stores.KeyValueFactory factory = Stores.create(name)
                .withKeys(keys)
                .withValues(values);
        return isMemory ? factory.inMemory().build() : factory.persistent().build();
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

    private static class NFAStateValue<K, V> implements Comparable<NFAStateValue>, Serializable {
        public Queue<ComputationStage<K, V>> computationStages;
        public Long runs;
        public Long latestOffset;

        public NFAStateValue(){}

        public NFAStateValue(Queue<ComputationStage<K, V>> computationStages, Long runs, Long latestOffset) {
            this.computationStages = computationStages;
            this.runs = runs;
            this.latestOffset = latestOffset;
        }

        @Override
        public int hashCode() {
            return latestOffset.hashCode();
        }

        @Override
        public int compareTo(NFAStateValue that) {
            return this.latestOffset.compareTo(that.latestOffset);
        }
    }

    private static class NFASTateValueSerDe<K, V> implements Serializer<NFAStateValue<K, V>>, Deserializer<NFAStateValue<K, V>> {

        private ComputationStageSerDe<K, V> computationStageSerDes;

        public NFASTateValueSerDe(ComputationStageSerDe<K, V> computationStageSerDes) {
            this.computationStageSerDes = computationStageSerDes;
        }

        @Override
        public NFAStateValue<K, V> deserialize(String topic, byte[] bytes) {
            Queue<ComputationStage<K, V>> queue = computationStageSerDes.deserialize(topic, bytes);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            buffer.put(Arrays.copyOfRange(bytes, bytes.length - Long.BYTES * 2, bytes.length));
            buffer.flip();
            long offset = buffer.getLong();
            long runs   = buffer.getLong();
            return new NFAStateValue<>(queue, runs, offset);
        }

        @Override
        public byte[] serialize(String topic, NFAStateValue<K, V> data) {
            byte[] stagesBytes = computationStageSerDes.serialize(topic, data.computationStages);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            buffer.putLong(data.latestOffset);
            buffer.putLong(data.runs);
            byte[] offsetBytes = buffer.array();

            byte[] bytes = new byte[stagesBytes.length + offsetBytes.length];
            System.arraycopy(stagesBytes, 0, bytes, 0, stagesBytes.length);
            System.arraycopy(offsetBytes, 0, bytes, stagesBytes.length, offsetBytes.length);
            return bytes;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }
        @Override
        public void close() {

        }
    }

        private static class TopicAndPartition implements Comparable<TopicAndPartition>, Serializable {

        public String topic;
        public int partition;

        /**
         * Dummy constructor for serialization.
         */
        public TopicAndPartition(){}

        TopicAndPartition(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicAndPartition that = (TopicAndPartition) o;
            return partition == that.partition &&
                    Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }

        @Override
        public int compareTo(TopicAndPartition that) {
            CompareToBuilder compareToBuilder = new CompareToBuilder();
            return compareToBuilder.append(this.topic, that.topic)
                    .append(this.partition, that.partition)
                    .build();
        }

        @Override
        public String toString() {
            return "TopicAndPartition{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    '}';
        }
    }
}
