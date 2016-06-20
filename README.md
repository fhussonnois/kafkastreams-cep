Complex Event Processing on top of KafkaStreams Processor API
=============================================================

###This repository is not stable - library is still in progress (use at your own risk)

## TODO / KNOWN ISSUES
 * NFA class is not tolerant to kafka rebalance operations.
 * A Stage state should be maintained per nfa run
 * NFA is not currently tolerant to at-least once semantic (keep a high water mark)

## Demonstration

The below example is based on the research paper **Efficient Pattern Matching over Event Streams**.

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
        Pattern<Object, StockEvent> pattern = new SequenceQuery<Object, StockEvent>()
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

        topologyBuilder.addStateStore(CEPProcessor.getEventsStore(true), "cep");    // required for buffer event matches
        topologyBuilder.addStateStore(CEPProcessor.getEventsStore(true), "volume"); // required for cep aggregates (i.e fold method)
        topologyBuilder.addStateStore(CEPProcessor.getEventsStore(true), "avg");    // required for cep aggregates (i.e fold method)

        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, streamingConfig);
        streaming.start();
```
## Support for event selection strategies
 * Strict contiguity
 * Skip till next match
 * Skip till any match
 
###Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License