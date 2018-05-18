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

import com.github.fhuss.kafka.streams.cep.state.internal.Aggregated;
import org.apache.kafka.streams.processor.StateStore;

/**
 * Interface for storing the aggregated values of pattern sequences.
 * 
 * @param <K>   the record key type
 */
public interface AggregatesStore<K> extends StateStore {

    <T> T find(final Aggregated<K> aggregated);

    <T> void put(final Aggregated<K> aggregated, final T aggregate);

    void branch(final Aggregated<K> aggregated, final long sequence);
}
