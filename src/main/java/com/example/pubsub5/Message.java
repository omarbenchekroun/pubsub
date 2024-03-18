package com.example.pubsub5;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

public class Message {
    private final static AtomicInteger counter = new AtomicInteger(0);

    public String content;
    public int msgNo;
    public Timestamp timestamp;
    public Message(String content) {
        this.content = content;
        this.msgNo = counter.getAndIncrement();
        this.timestamp = new Timestamp(System.currentTimeMillis());
    }
}
