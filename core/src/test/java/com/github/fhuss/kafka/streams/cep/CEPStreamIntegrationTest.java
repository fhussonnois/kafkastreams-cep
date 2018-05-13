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
package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.pattern.Aggregator;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.pattern.Selected;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CEPStreamIntegrationTest {

    private static final String APPLICATION_ID  = "streams-cep";
    private static final String INPUT_TOPIC_1   = "input_topic_1";
    private static final String INPUT_TOPIC_2   = "input_topic_2";
    private static final String OUTPUT_TOPIC_1  = "output_topic_1";


    private static final Pattern<String, Integer> SIMPLE_PATTERN = new QueryBuilder<String, Integer>()
            .select("stage-1")
                .where((event, states) -> event.value() == 0)
                .<Integer>fold("sum", (k, v, curr) -> v)
                .then()
            .select("stage-2")
                .oneOrMore()
                .where((event, states) -> ((int) states.get("sum")) <= 10)
                .<Integer>fold("sum", (k, v, curr) -> curr + v)
                .then()
            .select("stage-3")
                .where((event, states) -> ((int) states.get("sum")) + event.value() > 10)
            .within(1, TimeUnit.HOURS)
            .build();

    private static final Pattern<String, Integer> PATTERN_MULTIPLE_TOPICS = new QueryBuilder<String, Integer>()
            .select("stage-1", Selected.withStrictContiguity())
                .where((event, states) -> event.value() == 0)
                .<Integer>fold("sum", (k, v, curr) -> v)
                .then()
            .select("stage-2", Selected.withSkipTilNextMatch().withTopic(INPUT_TOPIC_1))
                .oneOrMore()
                .where((event, states) -> (Integer)states.get("sum") <= 10)
                .<Integer>fold("sum", (k, v, curr) -> curr + v)
                .then()
            .select("stage-3", Selected.withSkipTilAnyMatch().withTopic(INPUT_TOPIC_2))
                .where((event, states) -> event.value() >= ((Integer) states.get("sum")))
            .within(1, TimeUnit.HOURS)
            .build();


    private static final String K1 = "K1";
    private static final String K2 = "K2";

    private static final String STAGE_1 = "stage-1";
    private static final String STAGE_2 = "stage-2";
    private static final String STAGE_3 = "stage-3";

    private static final String DEFAULT_TEST_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String DEFAULT_TEST_QUERY = "test";

    private Properties streamsConfiguration = new Properties();
    private static final Serde<String> STRING_SERDE = Serdes.String();

    private ProcessorTopologyTestDriver driver;

    @Before
    public void before() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_TEST_BOOTSTRAP_SERVER);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("test").getPath());
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @After
    public void after() throws IOException {
        driver.close();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void testPatternGivenMultipleRecordKeys() {

        // Build query
        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, Integer> stream = builder.stream(INPUT_TOPIC_1, Consumed.with(STRING_SERDE, Serdes.Integer()));
        KStream<String, Sequence<String, Integer>> sequences = stream.query(DEFAULT_TEST_QUERY, SIMPLE_PATTERN,
                Queried.with(STRING_SERDE, Serdes.Integer()));

        sequences.to(OUTPUT_TOPIC_1, Produced.with(STRING_SERDE, new JsonSequenceSerde<>()));

        Topology topology = builder.build();

        StreamsConfig config = new StreamsConfig(streamsConfiguration);
        driver = new ProcessorTopologyTestDriver(config, topology);

        driver.process(INPUT_TOPIC_1, K1, 0,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K2, -10, STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K2, 0,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 3,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K2, 6,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 1,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 2,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 6,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K2, 4,   STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K2, 4,   STRING_SERDE.serializer(), Serdes.Integer().serializer());

        // JSON values are de-serialized as double
        List<ProducerRecord<String, Sequence<String, Double>>> results = new ArrayList<>();

        results.add(driver.readOutput(OUTPUT_TOPIC_1, STRING_SERDE.deserializer(), new JsonSequenceSerde.SequenceDeserializer<>()));
        results.add(driver.readOutput(OUTPUT_TOPIC_1, STRING_SERDE.deserializer(), new JsonSequenceSerde.SequenceDeserializer<>()));

        Assert.assertEquals(2, results.size());
        final ProducerRecord<String, Sequence<String, Double>> kvOne = results.get(0);
        Assert.assertEquals(K1, kvOne.key());
        assertStagesNames(kvOne.value(), STAGE_1, STAGE_2, STAGE_3);
        assertStagesValue(kvOne.value(), STAGE_1, 0.0);
        assertStagesValue(kvOne.value(), STAGE_2, 3.0, 1.0, 2.0);
        assertStagesValue(kvOne.value(), STAGE_3, 6.0);


        final ProducerRecord<String, Sequence<String, Double>> kvTwo = results.get(1);
        Assert.assertEquals(K2, kvTwo.key());
        assertStagesNames(kvTwo.value(), STAGE_1, STAGE_2, STAGE_3);
        assertStagesValue(kvTwo.value(), STAGE_1, 0.0);
        assertStagesValue(kvTwo.value(), STAGE_2, 6.0, 4.0);
        assertStagesValue(kvTwo.value(), STAGE_3, 4.0);

        driver.close();
    }

    @Test
    public void testPatternGivenRecordsFromMultipleTopics() {

        // Build query
        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, Integer> stream = builder.stream(Arrays.asList(INPUT_TOPIC_1, INPUT_TOPIC_2)
                , Consumed.with(STRING_SERDE, Serdes.Integer()));
        KStream<String, Sequence<String, Integer>> sequences = stream.query(DEFAULT_TEST_QUERY, PATTERN_MULTIPLE_TOPICS,
                Queried.with(STRING_SERDE, Serdes.Integer()));

        sequences.to(OUTPUT_TOPIC_1, Produced.with(STRING_SERDE, new JsonSequenceSerde<>()));

        Topology topology = builder.build();

        StreamsConfig config = new StreamsConfig(streamsConfiguration);
        driver = new ProcessorTopologyTestDriver(config, topology);

        driver.process(INPUT_TOPIC_1, K1, 0, STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 1, STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 2, STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_1, K1, 3, STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_2, K1, 6, STRING_SERDE.serializer(), Serdes.Integer().serializer());
        driver.process(INPUT_TOPIC_2, K1, 10, STRING_SERDE.serializer(), Serdes.Integer().serializer());

        // JSON values are de-serialized as double
        List<ProducerRecord<String, Sequence<String, Double>>> results = new ArrayList<>();

        ProducerRecord<String, Sequence<String, Double>> record;
        do {
            record = driver.readOutput(OUTPUT_TOPIC_1, STRING_SERDE.deserializer(), new JsonSequenceSerde.SequenceDeserializer<>());
            if (record != null) {
                results.add(record);
            }
        } while (record != null);

        results.forEach(System.out::println);
        Assert.assertEquals(2, results.size());

        final ProducerRecord<String, Sequence<String, Double>> first = results.get(0);
        Assert.assertEquals(K1, first.key());
        assertStagesNames(first.value(), STAGE_1, STAGE_2, STAGE_3);
        assertStagesValue(first.value(), STAGE_1, 0.0);
        assertStagesTopic(first.value(), STAGE_1, INPUT_TOPIC_1);
        assertStagesValue(first.value(), STAGE_2, 1.0, 2.0, 3.0);
        assertStagesTopic(first.value(), STAGE_2, INPUT_TOPIC_1, INPUT_TOPIC_1, INPUT_TOPIC_1);
        assertStagesValue(first.value(), STAGE_3, 6.0);
        assertStagesTopic(first.value(), STAGE_3, INPUT_TOPIC_2);

        final ProducerRecord<String, Sequence<String, Double>> second = results.get(1);
        Assert.assertEquals(K1, second.key());
        assertStagesNames(second.value(), STAGE_1, STAGE_2, STAGE_3);
        assertStagesValue(second.value(), STAGE_1, 0.0);
        assertStagesTopic(second.value(), STAGE_1, INPUT_TOPIC_1);
        assertStagesValue(second.value(), STAGE_2, 1.0, 2.0, 3.0);
        assertStagesTopic(second.value(), STAGE_2, INPUT_TOPIC_1, INPUT_TOPIC_1, INPUT_TOPIC_1);
        assertStagesValue(second.value(), STAGE_3, 10.0);
        assertStagesTopic(second.value(), STAGE_3, INPUT_TOPIC_2);

        driver.close();
    }

    private <K, V> void assertStagesNames(Sequence<K, V> sequence, String...stages) {
        List<String> expected = Arrays.asList(stages);
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i), sequence.getByIndex(i).getStage());
        }
    }

    private <K, V> void assertStagesValue(Sequence<K, V> sequence, String stage, V...values) {
        List<V> expected = Arrays.asList(values);
        Sequence.Staged<K, V> staged = sequence.getByName(stage);
        List<Event<K, V>> events = new ArrayList<>(staged.getEvents());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i).toString(), events.get(i).value().toString());
        }
    }

    private <K, V> void assertStagesTopic(Sequence<K, V> sequence, String stage, String...topics) {
        List<String> expected = Arrays.asList(topics);
        Sequence.Staged<K, V> staged = sequence.getByName(stage);
        List<Event<K, V>> events = new ArrayList<>(staged.getEvents());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i), events.get(i).topic());
        }
    }
}