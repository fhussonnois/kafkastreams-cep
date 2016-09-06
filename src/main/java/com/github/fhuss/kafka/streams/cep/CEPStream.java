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

import com.github.fhuss.kafka.streams.cep.pattern.PredicateBuilder;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.internals.AbstractStream;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;


public class CEPStream<K, V> extends AbstractStream<K> {

    private KStream<K, V> stream;

    /**
     * Creates a new {@link CEPStream} instance.
     * @param stream
     */
    public CEPStream(KStream<K, V> stream) {
        this((KStreamImpl)stream);
    }


    /**
     * Creates a new {@link CEPStream} instance.
     * @param stream
     */
    private CEPStream(KStreamImpl<K, V> stream) {
        super(stream);
        this.stream = stream;
    }

    public KStream<K, Sequence<K, V>> query(String queryName, PredicateBuilder<K, V> builder) {
        return query(queryName, builder.build());
    }

    public KStream<K, Sequence<K, V>> query(String queryName, Pattern<K, V> pattern) {
        String name = this.topology.newName("CEPSTREAM-QUERY-" + queryName);
        this.topology.addProcessor(name, () -> new CEPProcessor<>(queryName, pattern), this.name);
        return new KStreamImpl<>(this.topology, name, this.sourceNodes);
    }
}
