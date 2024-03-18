package com.example.pubsub5.callables;

import com.example.pubsub5.Message;
import com.example.pubsub5.Topic;
import com.example.pubsub5.User;
import org.springframework.web.socket.TextMessage;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class DispatchTopicsToUser implements Callable<Boolean> {
    private final User user;
    ConcurrentHashMap<String, Topic> topics;
    public DispatchTopicsToUser(ConcurrentHashMap<String, Topic> topics, User user){
        this.topics = topics;
        this.user = user;
    }
    public static String format(Topic topic, Message m){
        return "\n\n\nTopic: " + topic.name + "\nTime: " + new Timestamp(System.currentTimeMillis()) + "\nMessage: " + m.content;
    }
    @Override
    public Boolean call() throws IOException {
        for (Topic topic:  topics.values()){
            synchronized (topic){
                if (topic.subscribedUsers.containsKey(user)){
                    Iterator<Message> it = topic.UnreadMessagesIterator(user);
                    while(it.hasNext()){
                        Message m = it.next();
                        user.currentSession.sendMessage(new TextMessage(format(topic, m)));
                        topic.lastMessageReadByUser.put(user, m);
                    }
                }
            }
        }
        return true;
    }
}
