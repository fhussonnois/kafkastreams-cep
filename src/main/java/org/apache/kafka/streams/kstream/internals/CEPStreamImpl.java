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
package org.apache.kafka.streams.kstream.internals;

import com.github.fhuss.kafka.streams.cep.processor.CEPProcessor;
import com.github.fhuss.kafka.streams.cep.CEPStream;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.pattern.PredicateBuilder;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.state.CEPStoreBuilders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;


public class CEPStreamImpl<K, V> extends AbstractStream<K> implements CEPStream<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CEPStreamImpl.class);

    private KStream<K, V> stream;

    private CEPStoreBuilders<K, V> storeBuilders;

    /**
     * Creates a new {@link CEPStreamImpl} instance.
     * @param stream
     */
    public CEPStreamImpl(final KStream<K, V> stream) {
        this((KStreamImpl)stream);
    }

    /**
     * Creates a new {@link CEPStreamImpl} instance.
     * @param stream
     */
    private CEPStreamImpl(final KStreamImpl<K, V> stream) {
        super(stream);
        this.stream = stream;
        this.storeBuilders = new CEPStoreBuilders<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KStream<K, Sequence<K, V>> query(final String queryName,
                                            final PredicateBuilder<K, V> builder,
                                            final Serde<K> keySerde,
                                            final Serde<V> valueSerde) {
        return query(queryName, builder.build(), keySerde, valueSerde);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KStream<K, Sequence<K, V>> query(final String queryName,
                                            final Pattern<K, V> pattern,
                                            final Serde<K> keySerde,
                                            final Serde<V> valueSerde) {
        Objects.requireNonNull(queryName, "queryName can't be null");
        Objects.requireNonNull(pattern, "pattern can't be null");

        String name = builder.newProcessorName("CEPSTREAM-QUERY-" + queryName.toUpperCase() + "-");

        final ProcessorSupplier processor = () -> new CEPProcessor<>(queryName, pattern);
        builder.internalTopologyBuilder.addProcessor(name, processor, this.name);

        Collection<StoreBuilder> suppliers = storeBuilders.get(queryName, pattern, keySerde, valueSerde, false);
        for(StoreBuilder storeBuilder : suppliers) {
            LOG.info("State store registered with name {} for processor {}", storeBuilder.name(), name);
            builder.internalTopologyBuilder.addStateStore(storeBuilder, name);
        }

        return new KStreamImpl<>(builder, name, sourceNodes, true);
    }
}
