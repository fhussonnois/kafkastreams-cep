package com.github.fhuz.kafka.streams.cep.demo;


public class StockEvent {
    public String name;
    public long price;
    public long volume;

    public StockEvent(String name, long price, long volume) {
        this.name = name;
        this.price = price;
        this.volume = volume;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StockEvent{");
        sb.append("name='").append(name).append('\'');
        sb.append(", price=").append(price);
        sb.append(", volume=").append(volume);
        sb.append('}');
        return sb.toString();
    }
}
