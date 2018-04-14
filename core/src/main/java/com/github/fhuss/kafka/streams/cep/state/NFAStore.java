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

import com.github.fhuss.kafka.streams.cep.state.internal.NFAStates;
import com.github.fhuss.kafka.streams.cep.state.internal.Runned;
import org.apache.kafka.streams.processor.StateStore;

/**
 * Default interface to manage {@link NFAStates} storage.
 * @param <K>   the key type.
 * @param <V>   the value type.
 */
public interface NFAStore<K, V> extends StateStore {

    void put(final Runned key, final NFAStates<K, V> state);

    NFAStates<K, V> find(final Runned key);
}
