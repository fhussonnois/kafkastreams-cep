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

import org.apache.kafka.common.serialization.Serde;

/**
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class Queried<K, V> {

    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    /**
     * Creates a new {@link Queried} instance.
     *
     * @param keySerde      the record key serde
     * @param valueSerde    the record value serde
     */
    private Queried(final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Create an instance of {@code Queried} with  a key {@link Serde} and a value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
     *
     * @param keySerde      the record key serde
     * @param valueSerde    the record value serde
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     * @return new {@code Queried} instance configured with the keySerde and valueSerde
     */
    public static <K, V> Queried<K, V> with(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return new Queried<>(keySerde, valueSerde);
    }

    /**
     * Create an instance of {@code Queried} with  a key {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
     *
     * @param keySerde the key serde to use. If {@code null} the default key serde from config will be used
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     * @return new {@code Queried} instance configured with the keySerde
     */
    public static <K, V> Queried<K, V> keySerde(final Serde<K> keySerde) {
        return with(keySerde, null);
    }

    /**
     * Create an instance of {@code Queried} with a value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
     *
     * @param valueSerde the value serde to use. If {@code null} the default value serde from config will be used
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     * @return new {@code Queried} instance configured with the valueSerde
     */
    public static <K, V> Queried<K, V> valueSerde(final Serde<V> valueSerde) {
        return with(null, valueSerde);
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }
}
