package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.Selected;

import java.util.concurrent.TimeUnit;

public class Patterns {

    public static final Pattern<String, StockEvent> STOCKS = new QueryBuilder<String, StockEvent>()
            .select("stage-1")
                .where((event, states) -> event.value().volume > 1000)
                .<Long>fold("avg", (k, v, curr) -> v.price)
                .then()
            .select("stage-2", Selected.withSkipTilNextMatch())
                .zeroOrMore()
                .where((event, states) -> event.value().price > (long) states.get("avg"))
                .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                .<Long>fold("volume", (k, v, curr) -> v.volume)
                .then()
            .select("stage-3", Selected.withSkipTilNextMatch())
                .where((event, states) -> event.value().volume < 0.8 * (long) states.getOrElse("volume", 0L))
            .within(1, TimeUnit.HOURS)
            .build();
}
