package com.example.pubsub5;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Topic {
    public String name;
    public LinkedBlockingQueue<PubSub5Application.Msg> messages ;
    public ConcurrentHashMap<PubSub5Application.User, Boolean> subscribed;
    public ConcurrentHashMap<PubSub5Application.User, PubSub5Application.Msg> lastMsgRead;

    public Topic(String name){
        this.name = name;
        this.messages = new LinkedBlockingQueue<>();
        this.lastMsgRead = new ConcurrentHashMap<>();
        this.subscribed = new ConcurrentHashMap<>();
    }
    public Iterator<PubSub5Application.Msg> UnreadMessagesIterator(PubSub5Application.User u){
        Iterator<PubSub5Application.Msg> it = messages.iterator();
        if (lastMsgRead.containsKey(u)){
            PubSub5Application.Msg lastMsg = lastMsgRead.get(u);
            while (it.hasNext()){
                if (it.next().equals(lastMsg)){
                    break;
                }
            }
        }
        return it;
    }

    public void publish(PubSub5Application.Msg m) {
        messages.offer(m);
    }

    public void subscribe(PubSub5Application.User u) {
        subscribed.put(u, true);
    }

    public void unsubscribe(PubSub5Application.User u) {
        subscribed.remove(u);
        lastMsgRead.remove(u);
    }
}
