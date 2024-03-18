package com.example.pubsub5;

import org.springframework.web.socket.WebSocketSession;

import java.util.concurrent.atomic.AtomicInteger;

public class User {
    public static AtomicInteger counter = new AtomicInteger(0);
    public String name;
    public int id;
    public WebSocketSession currentSession ;

    public User(String name) {
        this.name = name;
        this.id = counter.getAndIncrement();
        this.currentSession = null;
    }

    public String getWebSocketURL(){
        return String.format("ws://localhost:8080/%d", id);
    }
}
