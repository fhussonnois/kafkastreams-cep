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

import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.PredicateBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Default interface to build a CEP Streams.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public interface CEPStream<K, V> {

    /**
     *
     * @param queryName     the query name.
     * @param builder       the builder to builder the pattern to query.
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    default KStream<K, Sequence<K, V>> query(final String queryName,  final PredicateBuilder<K, V> builder) {
        return query(queryName, builder, null);
    }

    /**
     *
     * @param queryName     the query name.
     * @param builder       the builder to builder the pattern to query.
     * @param queried
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    default KStream<K, Sequence<K, V>> query(final String queryName,
                                     final PredicateBuilder<K, V> builder,
                                     final Queried<K, V> queried) {
        return query(queryName, builder.build(), queried);
    }


    /**
     *
     * @param queryName     the query name.
     * @param pattern       the pattern to query.
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    default KStream<K, Sequence<K, V>> query(final String queryName, final Pattern<K, V> pattern) {
        return query(queryName, pattern, null);
    }

    /**
     *
     * @param queryName     the query name.
     * @param pattern       the pattern to query.
     * @param queried
     * @return a {@link KStream} that contains only those records that satisfy the given pattern with unmodified keys and new values.
     */
    KStream<K, Sequence<K, V>> query(final String queryName,
                                     final Pattern<K, V> pattern,
                                     final Queried<K, V> queried);
}
