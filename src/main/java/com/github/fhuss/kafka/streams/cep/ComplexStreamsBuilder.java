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
package com.github.fhuss.kafka.streams.cep;

import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.internals.CEPStreamImpl;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class ComplexStreamsBuilder {

    private StreamsBuilder builder;

    /**
     * Creates a new {@link ComplexStreamsBuilder} instance.
     */
    public ComplexStreamsBuilder() {
        this.builder = new StreamsBuilder();
    }

    /**
     * Creates a new {@link ComplexStreamsBuilder} instance for the specified {@link StreamsBuilder}.
     *
     * @param builder  the streams builder used to create the underlying streams.
     */
    public ComplexStreamsBuilder(final StreamsBuilder builder) {
        this.builder = builder;
    }

    /**
     * Create a {@link CEPStream} from the specified topic {@link StreamsBuilder#stream(Collection, Consumed)}
     *
     * @return a {@link CEPStream} for the specified topics
     */
    public <K, V> CEPStream<K, V> stream(final Collection<String> topics,
                                         final Consumed<K, V> consumed)  {
        return stream(builder.stream(topics, consumed));
    }

    /**
     * Create a {@link CEPStream} from the specified topic {@link StreamsBuilder#stream(String)}.
     *
     * @return a {@link CEPStream} for the specified topic
     */
    public <K, V> CEPStream<K, V> stream(final String topic) {
        return stream(builder.stream(Collections.singleton(topic)));
    }

    /**
     * Create a {@link CEPStream} from the specified {@link KStream}.
     *
     * @return a {@link CEPStream} for the specified stream.
     */
    public <K, V> CEPStream<K, V> stream(KStream<K, V> stream) {
        return new CEPStreamImpl<>(stream);
    }

    public Topology build() {
        return builder.build();
    }
}
