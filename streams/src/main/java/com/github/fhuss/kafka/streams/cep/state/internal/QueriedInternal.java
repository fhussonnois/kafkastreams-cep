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
package com.github.fhuss.kafka.streams.cep.state.internal;

import com.github.fhuss.kafka.streams.cep.Queried;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

import java.util.Map;

public class QueriedInternal<K, V> extends Queried<K, V> {

    public QueriedInternal() {
        super();
    }

    public QueriedInternal(final Queried<K, V> queried) {
        super(queried);
    }

    public StoreSupplier<KeyValueStore<Bytes, byte[]>>  storeSupplier() {
        return storeSupplier;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    public boolean cachingEnabled() {
        return cachingEnabled;
    }

    public Map<String, String> logConfig() {
        return topicConfig;
    }


}
