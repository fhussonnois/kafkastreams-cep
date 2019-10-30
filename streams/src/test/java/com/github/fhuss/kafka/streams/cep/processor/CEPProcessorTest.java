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
package com.github.fhuss.kafka.streams.cep.processor;

import com.github.fhuss.kafka.streams.cep.CapturedProcessorContext;
import com.github.fhuss.kafka.streams.cep.Queried;
import com.github.fhuss.kafka.streams.cep.core.nfa.Stages;
import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.PatternBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.StagesFactory;
import com.github.fhuss.kafka.streams.cep.state.AggregatesStateStore;
import com.github.fhuss.kafka.streams.cep.state.NFAStateStore;
import com.github.fhuss.kafka.streams.cep.state.QueryStoreBuilders;
import com.github.fhuss.kafka.streams.cep.state.SharedVersionedBufferStateStore;
import com.github.fhuss.kafka.streams.cep.state.internal.QueriedInternal;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class CEPProcessorTest {

    private static final String TOPIC_TEST_1         = "topic-test-1";
    private static final String TOPIC_TEST_2         = "topic-test-2";
    private static final String TEST_RECORD_KEY      = "test-key";
    private static final String TEST_RECORD_VALUE    = "test-value";
    private static final String TEST_QUERY           = "test-query";

    private static final PatternBuilder<String, String> PATTERN =  new QueryBuilder<String, String>()
            .select()
            .where((event) -> true);

    private Pattern<String, String> pattern;

    private StoreBuilder<NFAStateStore<String, String>> nfaStateStore ;
    private StoreBuilder<SharedVersionedBufferStateStore<String, String>> eventBufferStore ;
    private StoreBuilder<AggregatesStateStore<String>> aggregateStateStores;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void before() {
        pattern = PATTERN.build();

        StagesFactory<String, String> factory = new StagesFactory<>();
        Stages<String, String> stages = factory.make(pattern);

        final QueryStoreBuilders<String, String> qsb = new QueryStoreBuilders<>(TEST_QUERY, pattern);
        final Queried<String, String> queried = new QueriedInternal<>();
        nfaStateStore = qsb.getNFAStateStore(queried);
        eventBufferStore = qsb.getEventBufferStore(queried);
        aggregateStateStores = qsb.getAggregateStateStore(queried);

    }

    @Test
    public void shouldMeterOnSkippedRecordsWithNullValue() {

        StreamsBuilder builder = new StreamsBuilder();
        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(
            new StringSerializer(),
            new StringSerializer()
        );

        KStream<String, String> stream = builder.stream(TOPIC_TEST_1);

        builder.addStateStore(nfaStateStore);
        builder.addStateStore(eventBufferStore);
        builder.addStateStore(aggregateStateStores);

        final String[] stateStoreNames = new String[]{
            nfaStateStore.name(),
            eventBufferStore.name(),
            aggregateStateStores.name()
        };
        stream.process(() -> new CEPProcessor<>(TEST_QUERY, pattern), stateStoreNames);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), this.props)) {
            driver.pipeInput(recordFactory.create(TOPIC_TEST_1, "A", (String)null));
            Assert.assertEquals(1.0D,
            StreamsTestUtils.getMetricByName(
                driver.metrics(),
                "skipped-records-total", "stream-metrics")
            .metricValue());
        }
    }

    @Test
    public void shouldForwardMatchingSequencesToDownStreamsProcessors() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(Arrays.asList(TOPIC_TEST_1, TOPIC_TEST_2));

        builder.addStateStore(nfaStateStore);
        builder.addStateStore(eventBufferStore);
        builder.addStateStore(aggregateStateStores);

        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(
                new StringSerializer(),
                new StringSerializer()
        );

        final String[] stateStoreNames = new String[]{
                nfaStateStore.name(),
                eventBufferStore.name(),
                aggregateStateStores.name()
        };
        CapturedCEProcessor<String, String> processor = new CapturedCEProcessor<>(TEST_QUERY, pattern);
        stream.process(() -> processor, stateStoreNames);

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), this.props)) {
            driver.pipeInput(recordFactory.create(TOPIC_TEST_1, TEST_RECORD_KEY, TEST_RECORD_VALUE));
            driver.pipeInput(recordFactory.create(TOPIC_TEST_2, TEST_RECORD_KEY, TEST_RECORD_VALUE));
        }

        List<CapturedProcessorContext.CapturedForward> capturedForward = processor.context.capturedForward;
        Assert.assertEquals(2, capturedForward.size());
    }


    private static final class CapturedCEProcessor<K, V> extends CEPProcessor<K, V> {

        private CapturedProcessorContext context;

        CapturedCEProcessor(final String queryName, final Pattern<K, V> pattern) {
            super(queryName, pattern);
        }

        public void init(final ProcessorContext context) {
            super.init(context);
            this.context = new CapturedProcessorContext(context);
        }

        ProcessorContext context() {
            return context;
        }


    }

}
