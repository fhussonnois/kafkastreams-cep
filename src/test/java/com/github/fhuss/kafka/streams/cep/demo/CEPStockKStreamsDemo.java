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
package com.github.fhuss.kafka.streams.cep.demo;

import com.github.fhuss.kafka.streams.cep.CEPStream;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CEPStockKStreamsDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-cep");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, StockEventSerDe.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StockEventSerDe.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // build query
        final Pattern<Object, StockEvent> pattern = new QueryBuilder<Object, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Long>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .zeroOrMore()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.price > (long)state.get("avg"))
                    .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                    .<Long>fold("volume", (k, v, curr) -> v.volume)
                    .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < 0.8 * state.getOrElse("volume", 0L))
                    .within(1, TimeUnit.HOURS)
                .build();

        KStreamBuilder builder = new KStreamBuilder();

        CEPStream<Object, StockEvent> stream = new CEPStream<>(builder.stream("StockEvents"));

        KStream<Object, Sequence<Object, StockEvent>> stocks = stream.query("Stocks", pattern);

        stocks.mapValues(seq -> {
                  JSONObject json = new JSONObject();
                  seq.asMap().forEach( (k, v) -> {
                      JSONArray events = new JSONArray();
                      json.put(k, events);
                      List<String> collect = v.stream().map(e -> e.value.name).collect(Collectors.toList());
                      Collections.reverse(collect);
                      collect.forEach(e -> events.add(e));
                  });
                  return json.toJSONString();
              })
              .through(null, Serdes.String(), "Matches")
              .print();


        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(builder, props);
        //streaming.cleanUp();
        streaming.start();
    }
}
