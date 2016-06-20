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
package com.github.fhuz.kafka.streams.cep.pattern;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.UUID;

/**
 * Simple class to wrap a {@link KeyValueStore}.
 */
public class States {

    private final ProcessorContext context;

    private UUID version;

    /**
     * Creates a new {@link States} instance.
     * @param context
     * @param version
     */
    public States(ProcessorContext context, UUID version) {
        this.context = context;
        this.version = version;
    }

    /**
     * @see {@link KeyValueStore#get(Object)}
     */
    public Object get(String key) {
        ValueStore store = newValueStore(key);
        return ( store != null ) ? store.get() : null;
    }

    /**
     * @see {@link KeyValueStore#get(Object)}
     */
    @SuppressWarnings("unchecked")
    public <T> T getOrElse(String key, T def) {
        ValueStore store = newValueStore(key);
        if ( store != null ) {
            T val = (T) store.get();
            return val != null ? val : def;
        }
        else return def;
    }

    @SuppressWarnings("unchecked")
    private ValueStore newValueStore(String state) {
        KeyValueStore store = (KeyValueStore) context.getStateStore(state);
        return ( store != null ) ? new ValueStore(context.topic(), context.partition(), version, store) : null;
    }
}
