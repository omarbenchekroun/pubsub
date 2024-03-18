package com.example.pubsub5;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Topic {
    public String name;
    public LinkedBlockingQueue<Message> messages ;
    public ConcurrentHashMap<User, Boolean> subscribedUsers;
    public ConcurrentHashMap<User, Message> lastMessageReadByUser;

    public Topic(String name){
        this.name = name;
        this.messages = new LinkedBlockingQueue<>();
        this.lastMessageReadByUser = new ConcurrentHashMap<>();
        this.subscribedUsers = new ConcurrentHashMap<>();
    }
    public Iterator<Message> UnreadMessagesIterator(User u){
        Iterator<Message> it = messages.iterator();
        if (lastMessageReadByUser.containsKey(u)){
            Message lastMsg = lastMessageReadByUser.get(u);
            while (it.hasNext()){
                if (it.next().equals(lastMsg)){
                    break;
                }
            }
        }
        return it;
    }

    public void publish(Message m) {
        messages.offer(m);
    }

    public void subscribe(User u) {
        subscribedUsers.put(u, true);
    }

    public void unsubscribe(User u) {
        subscribedUsers.remove(u);
        lastMessageReadByUser.remove(u);
    }
}
