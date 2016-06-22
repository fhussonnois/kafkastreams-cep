Complex Event Processing on top of Kafka Streams Processor API !
=============================================================
![build status](https://travis-ci.org/fhussonnois/kafkastreams-cep.svg?branch=master)

[Apache Kafka](http://kafka.apache.org/) is a high-throughput, distributed, publish-subscribe messaging system.

This repository is not stable - This library is still in progress (use at your own risk)


## Demonstration

The below example is based on the research paper **Efficient Pattern Matching over Event Streams**.

Implementation based on https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf

### CEP Query : 

```
     PATTERN SEQ(Stock+ a[ ], Stock b)
       WHERE skip_till_next_match(a[ ], b) {
           [symbol]
       and
           a[1].volume > 1000
       and
           a[i].price > avg(a[..i-1].price)
       and
           b.volume < 80%*a[a.LEN].volume }
       WITHIN 1 hour
```
### KafkaStreams implementation:

```java
        Pattern<Object, StockEvent> pattern = new QueryBuilder<Object, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Integer>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .oneOrMore()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.price > (int)state.get("avg"))
                    .<Integer>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                    .<Integer>fold("volume", (k, v, curr) -> v.volume)
                    .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < (0.8 *  (int)state.get("volume")))
                    .within(1, TimeUnit.HOURS)
                .build();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("source", "StockEvents")
                .addProcessor("cep", () -> new CEPProcessor<>(query), "source");

        // Required for buffer event matches
        topologyBuilder.addStateStore(CEPProcessor.getEventsStore(true), "cep");

        // Aggregates states (i.e fold method)
        topologyBuilder.addStateStore(newStateStore("volume", true), "cep");
        topologyBuilder.addStateStore(newStateStore("avg", true), "cep");

        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, props);
        streaming.start();
```

## Demo

Run the demonstration class **CEPStockKStreamsDemo** :

- Produce the following json events **StockEvents**:
```bash
./bin/kafka-console-producer --topic StockEvents --broker-list localhost:9092
```

- Input

```json

{"name":"e1","price":100,"volume":1010}
{"name":"e2","price":120,"volume":990}
{"name":"e3","price":120,"volume":1005}
{"name":"e4","price":121,"volume":999}
{"name":"e5","price":120,"volume":999}
{"name":"e6","price":125,"volume":750}
{"name":"e7","price":120,"volume":950}
{"name":"e8","price":120,"volume":700}
    
```


- Consume from the sink topic **"matches"**

```bash
./bin/kafka-console-consumer --new-consumer --topic matches --bootstrap-server localhost:9092
```
- Output

```json
{"0":["e1"],"1":["e2","e3","e4","e5"],"2":["e6"]}
{"0":["e3"],"1":["e4"],"2":["e6"]}
{"0":["e1"],"1":["e2","e3","e4","e5","e6","e7"],"2":["e8"]}
{"0":["e3"],"1":["e4","e6"],"2":["e8"]}
```

## Support for event selection strategies
 * Strict contiguity
 * Skip till next match
 * Skip till any match
 

### TODO
 * Improve test scenarios
 * NFA class is not tolerant to kafka rebalance operations.
 * A Stage state should be maintained per nfa run
 * NFA is not currently tolerant to at-least once semantic (keep a high water mark) 
 
###Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License