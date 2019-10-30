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
package com.github.fhuss.kafka.streams.cep;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.ToInternal;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CapturedProcessorContext implements ProcessorContext {

    private final ProcessorContext context;

    public List<CapturedForward> capturedForward = new ArrayList<>();

    public CapturedProcessorContext(final ProcessorContext context) {
        this.context = context;
    }


    @Override
    public String applicationId() {
        return context.applicationId();
    }

    @Override
    public TaskId taskId() {
        return context.taskId();
    }

    @Override
    public Serde<?> keySerde() {
        return context.keySerde();
    }

    @Override
    public Serde<?> valueSerde() {
        return context.valueSerde();
    }

    @Override
    public File stateDir() {
        return context.stateDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return context.metrics();
    }

    @Override
    public void register(StateStore store, StateRestoreCallback stateRestoreCallback) {
        context.register(store, stateRestoreCallback);
    }

    @Override
    public StateStore getStateStore(String name) {
        return context.getStateStore(name);
    }

    @Override
    @Deprecated
    public Cancellable schedule(long intervalMs, PunctuationType type, Punctuator callback) {
        return context.schedule(intervalMs, type, callback);
    }

    @Override
    public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) throws IllegalArgumentException {
        return context.schedule(interval, type, callback);
    }

    @Override
    public <K, V> void forward(K key, V value) {
        capturedForward.add(new CapturedForward(To.all(), KeyValue.pair(key, value)));
        context.forward(key, value);
    }

    @Override
    public <K, V> void forward(K key, V value, To to) {
        capturedForward.add(new CapturedForward(to, KeyValue.pair(key, value)));
        context.forward(key, value, to);
    }

    @Override
    @Deprecated
    public <K, V> void forward(K key, V value, int childIndex) {
        context.forward(key, value, childIndex);
    }

    @Override
    @Deprecated
    public <K, V> void forward(K key, V value, String childName) {
        context.forward(key, value, childName);
    }

    @Override
    public void commit() {
        context.commit();
    }

    @Override
    public String topic() {
        return context.topic();
    }

    @Override
    public int partition() {
        return context.partition();
    }

    @Override
    public long offset() {
        return context.offset();
    }

    @Override
    public Headers headers() {
        return context.headers();
    }

    @Override
    public long timestamp() {
        return context.timestamp();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return context.appConfigs();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String prefix) {
        return context.appConfigsWithPrefix(prefix);
    }

    public static class CapturedForward {
        private final String childName;
        private final long timestamp;
        private final KeyValue keyValue;

        private CapturedForward(To to, KeyValue keyValue) {
            if (keyValue == null) {
                throw new IllegalArgumentException();
            } else {
                ToInternal internal = new ToInternal();
                internal.update(to);
                this.childName = internal.child();
                this.timestamp = internal.timestamp();
                this.keyValue = keyValue;
            }
        }

        public String childName() {
            return this.childName;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public KeyValue keyValue() {
            return this.keyValue;
        }
    }
}