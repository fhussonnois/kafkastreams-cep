package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;

import java.util.concurrent.TimeUnit;

public class Patterns {

    public static final Pattern<String, StockEvent> STOCKS = new QueryBuilder<String, StockEvent>()
            .select("stage-1")
                .where((k, v, ts, store) -> v.volume > 1000)
                .<Long>fold("avg", (k, v, curr) -> v.price)
                .then()
            .select("stage-2")
                .zeroOrMore()
                .skipTillNextMatch()
                .where((k, v, ts, state) -> v.price > (long) state.get("avg"))
                .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                .<Long>fold("volume", (k, v, curr) -> v.volume)
                .then()
            .select("stage-3")
                .skipTillNextMatch()
                .where((k, v, ts, state) -> v.volume < 0.8 * (long) state.getOrElse("volume", 0L))
            .within(1, TimeUnit.HOURS)
            .build();
}
