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
package com.github.fhuss.kafka.streams.cep.state;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class StateStoreProvider {

    private static final String PREFIX_BUFFER_EVENT_STORE = "cep-evt-";

    private static final String PREFIX_NFA_STORE          = "cep-nfa-";

    private static final String PREFIX_STATES_STORE       = "cep-state-";

    private String pattern;

    private ProcessorContext context;

    /**
     * Creates a new {@link StateStoreProvider} instance.
     * @param queryName
     * @param context
     */
    public StateStoreProvider(String queryName, ProcessorContext context) {
        this.pattern = queryName;
        this.context = context;
    }

    /**
     * Retrieve the name of the state used to store query states.
     */
    public static String getStateStoreName(String queryName, String stateName) {
        return makeStateName(PREFIX_STATES_STORE, queryName, stateName);
    }
    /**
     * Retrieve the name of the state used to store events which matched.
     */
    public static String getEventBufferStoreName(String queryName) {
        return makeStateName(PREFIX_BUFFER_EVENT_STORE, queryName, null);
    }
    /**
     * Retrieve the name of the state used to store NFA.
     */
    public static String getNFAStoreName(String queryName) {
        return makeStateName(PREFIX_NFA_STORE, queryName, null);
    }

    @SuppressWarnings("unchecked")
    public <V> ValueStore<V> getValueStore(String state, long sequence) {
        KeyValueStore store = (KeyValueStore) context.getStateStore(makeStateName(PREFIX_STATES_STORE, pattern, state));
        return ( store != null ) ? new ValueStore(context.topic(), context.partition(), sequence, store) : null;
    }

    private static String makeStateName(String type, String queryName, String state) {
        String s = type + queryName;
        return (state == null ? s : s + "-" + state).toLowerCase();
    }
}
