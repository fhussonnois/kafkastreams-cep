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
package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStateStore;
import com.github.fhuss.kafka.streams.cep.state.NFAStateStore;
import com.github.fhuss.kafka.streams.cep.state.QueryStoreBuilders;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStateStore;
import com.github.fhuss.kafka.streams.cep.state.internal.QueriedInternal;
import com.github.fhuss.kafka.streams.cep.transformer.CEPStreamTransformer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.internals.AbstractStream;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Objects;

public class CEPStreamImpl<K, V> extends AbstractStream<K, V> implements CEPStream<K, V> {

    private final KStream<K, V> stream;

    /**
     * Creates a new {@link CEPStreamImpl} instance.
     *
     * @param stream    the {@link KStream} instance.
     */
    @SuppressWarnings("unchecked")
    CEPStreamImpl(final KStream<K, V> stream) {
        this((KStreamImpl)stream);
    }

    /**
     * Creates a new {@link CEPStreamImpl} instance.
     *
     * @param stream    the {@link KStream} instance.
     */
    private CEPStreamImpl(final KStreamImpl<K, V> stream) {
        super(stream);
        this.stream = stream;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public KStream<K, Sequence<K, V>> query(final String queryName,
                                            final Pattern<K, V> pattern,
                                            final Queried<K, V> queried) {
        Objects.requireNonNull(queryName, "queryName can't be null");
        Objects.requireNonNull(pattern, "pattern can't be null");

        final QueryStoreBuilders<K, V> qsb = new QueryStoreBuilders<>(queryName, pattern);

        final Queried<K, V> internal = queried == null ? new QueriedInternal<>() : queried;

        final StoreBuilder<NFAStateStore<K, V>> nfaStateStore = qsb.getNFAStateStore(internal);
        final StoreBuilder<SharedVersionedBufferStateStore<K, V>> eventBufferStore = qsb.getEventBufferStore(internal);
        final StoreBuilder<AggregatesStateStore<K>> aggregateStateStores = qsb.getAggregateStateStore(internal);

        builder.addStateStore(nfaStateStore);
        builder.addStateStore(eventBufferStore);
        builder.addStateStore(aggregateStateStores);

        final String[] stateStoreNames = new String[]{
            nfaStateStore.name(),
            eventBufferStore.name(),
            aggregateStateStores.name()
        };

        return stream.flatTransformValues(() -> new CEPStreamTransformer<>(queryName, pattern), stateStoreNames);
    }
}
