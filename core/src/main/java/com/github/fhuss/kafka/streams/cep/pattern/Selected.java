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
package com.github.fhuss.kafka.streams.cep.pattern;

public class Selected {

    private final Strategy strategy;

    /**
     * Creates a new {@link Selected} instance.
     *
     * @param strategy the strategy used to selected relevant events
     */
    private Selected(final Strategy strategy) {
        this.strategy = strategy;
    }

    public static Selected withStrictContiguity() {
        return new Selected(Strategy.STRICT_CONTIGUITY);
    }

    public static Selected withSkipTilAnyMatch() {
        return new Selected(Strategy.SKIP_TIL_ANY_MATCH);
    }

    public static Selected withSkipTilNextMatch() {
        return new Selected(Strategy.SKIP_TIL_NEXT_MATCH);
    }

    public static Selected withStrategy(final Strategy strategy) {
        return new Selected(strategy);
    }

    public Strategy getStrategy() {
        return strategy;
    }
}
