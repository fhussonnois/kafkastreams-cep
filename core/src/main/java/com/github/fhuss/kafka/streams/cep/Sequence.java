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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A completed sequence of matching events.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class Sequence<K, V> implements Iterable<Event<K, V>> {

    private Collection<Staged<K, V>> matched;

    private Map<String, Staged<K, V>> indexed;

    /**
     * Creates a new {@link Sequence} instance.
     *
     * @param matched   list of matching stages.
     */
    public Sequence(final Collection<Staged<K, V>> matched) {
        this.matched = matched;
        this.indexed = matched.stream().collect(Collectors.toMap(Staged::getStage, e -> e));
    }
    /**
     * Returns the all events that match the specified stage.
     *
     * @param   stage the name of the stage.
     * @return  the staged instance or <code>null</code>.
     */
    public Staged<K, V> getByName(final String stage) {
        return this.indexed.get(stage);
    }

    /**
     * Returns the all events that match the specified stage.
     * @param   stage the index of the stage.
     * @return  the staged instance or <code>null</code>.
     */
    public Staged<K, V> getByIndex(final int stage) {
        return new ArrayList<>(this.matched).get(stage);
    }


    public Collection<Staged<K, V>> matched() {
        return this.matched;
    }

    /**
     * Returns the number of matching events contains into this sequence.
     * @return number of matching events.
     */
    public long size() {
        return matched
                .stream()
                .mapToInt(s -> s.getEvents().size()).sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sequence<?, ?> that = (Sequence<?, ?>) o;
        return Objects.equals(new ArrayList<>(this.matched), that.matched);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(matched);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return matched.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Event<K, V>> iterator() {
        LinkedList<Event<K, V>> l = new LinkedList<>();
        for (Staged<K, V> staged : matched) {
            l.addAll(staged.events);
        }
        return l.iterator();
    }

    /**
     * A completed stage of matching events.
     *
     * @param <K>   the record key type.
     * @param <V>   the record value type.
     */
    public static class Staged<K, V> {

        private final String stage;
        private final Collection<Event<K, V>> events;


        /**
         * Creates a new {@link Staged} instance.
         *
         * @param stage     name of the stage.
         */
        Staged(final String stage) {
            this(stage, new TreeSet<>());
        }

        /**
         * Creates a new {@link Staged} instance.
         *
         * @param stage     name of the stage.
         * @param events    events that matched this stage.
         */
        Staged(final String stage, final Collection<Event<K, V>> events) {
            this.stage = stage;
            this.events = events;
        }

        void add(final Event<K, V> event) {
            this.events.add(event);
        }

        public String getStage() {
            return stage;
        }

        public Collection<Event<K, V>> getEvents() {
            return new TreeSet<>(this.events);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Staged<?, ?> staged = (Staged<?, ?>) o;
            return Objects.equals(stage, staged.stage) &&
                    Objects.equals(events, staged.events);
        }

        @Override
        public int hashCode() {

            return Objects.hash(stage, events);
        }

        @Override
        public String toString() {
            return "{" +
                    "stage='" + stage + '\'' +
                    ", events=" + events +
                    '}';
        }
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {

        private LinkedHashMap<String, Staged<K, V>> matched = new LinkedHashMap<>();

        public Builder<K, V> add(final String stage, final Event<K, V> event) {
            Staged<K, V> staged = matched.get(stage);
            if( staged == null) {
                staged = new Staged<>(stage);
                this.matched.put(stage, staged);
            }
            staged.add(event);
            return this;
        }

        public Sequence<K, V> build(boolean reversed) {
            return reversed ? reverse() : new Sequence<>(matched.values());
        }

        private Sequence<K, V> reverse() {
            ListIterator<String> it = new LinkedList<>(this.matched.keySet()).listIterator(this.matched.size());

            Collection<Staged<K, V>> reordered = new LinkedList<>();
            while (it.hasPrevious()) {
                String stage = it.previous();
                reordered.add(this.matched.get(stage));
            }
            return new Sequence<>(reordered);
        }
    }
}
