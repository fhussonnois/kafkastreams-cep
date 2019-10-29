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
package com.github.fhuss.kafka.streams.cep.transformer;

import com.github.fhuss.kafka.streams.cep.core.Sequence;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.processor.CEPProcessor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CEPStreamTransformer<K, V> implements ValueTransformerWithKey<K, V, Iterable<Sequence<K, V>>> {

    private CEPProcessor<K, V> internal;

    /**
     * Creates a new {@link CEPStreamTransformer} instance.
     * @param queryName     the query name.
     * @param queryPattern  the query {@link Pattern}.
     */
    public CEPStreamTransformer(final String queryName,
                                final Pattern<K, V> queryPattern) {
        internal = new CEPProcessor<>(queryName, queryPattern);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final ProcessorContext context) {
        internal.init(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Sequence<K, V>> transform(final K k, final V v) {
        return internal.match(k, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        internal.close();
    }
}
