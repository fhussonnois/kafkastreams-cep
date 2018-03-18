package com.github.fhuss.kafka.streams.cep.demo;

import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;

import java.util.concurrent.TimeUnit;

public class Patterns {

    public static final com.github.fhuss.kafka.streams.cep.pattern.Pattern<String, StockEvent> STOCKS = new QueryBuilder<String, StockEvent>()
            .select()
            .where((k, v, ts, store) -> v.volume > 1000)
            .<Long>fold("avg", (k, v, curr) -> v.price)
            .then()
            .select()
            .zeroOrMore()
            .skipTillNextMatch()
            .where((k, v, ts, state) -> v.price > (long) state.get("avg"))
            .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
            .<Long>fold("volume", (k, v, curr) -> v.volume)
            .then()
            .select()
            .skipTillNextMatch()
            .where((k, v, ts, state) -> v.volume < 0.8 * (long) state.getOrElse("volume", 0L))
            .within(1, TimeUnit.HOURS)
            .build();
}
