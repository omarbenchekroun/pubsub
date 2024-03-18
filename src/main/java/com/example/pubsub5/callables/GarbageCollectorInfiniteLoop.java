package com.example.pubsub5.callables;

import com.example.pubsub5.Message;
import com.example.pubsub5.Topic;
import com.example.pubsub5.User;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.example.pubsub5.PubSubApplication.GARBAGE_COLLECTOR_INTERVAL_MILLISECONDS;
import static com.example.pubsub5.PubSubApplication.MESSAGE_TTL_MILLISECONDS;

@Slf4j
public class GarbageCollectorInfiniteLoop implements Callable<Boolean> {
    ConcurrentHashMap<String, Topic> topics;
    public GarbageCollectorInfiniteLoop(ConcurrentHashMap<String, Topic> topics){
        this.topics = topics;
    }
    @Override
    public Boolean call() throws InterruptedException {
        while (true){
            log.info("Attempting to clean messages");
            for (Topic topic:topics.values()){
                synchronized (topic){
                    Timestamp now = new Timestamp(System.currentTimeMillis());
                    // The rules stipulate that a message is deleted if:
                    // 1 -  Time to Live (TTL) has elapsed.
                    Optional<Message> minMsg1 = topic.messages.stream().filter(m->(now.getTime()-m.timestamp.getTime()>MESSAGE_TTL_MILLISECONDS)).reduce((first, second)->second);
                    // 2 - All users have received it.
                    Optional<Message> minMsg2 = Optional.empty();
                    if (topic.subscribedUsers.keySet().size()==topic.lastMessageReadByUser.keySet().size()) {
                        minMsg2 = topic.lastMessageReadByUser.values().stream().min(Comparator.comparingInt(a -> a.msgNo));
                    }
                    // OR Condition
                    Optional<Optional<Message>> minMsg = Stream.of(minMsg1, minMsg2).filter(m->m.isPresent()).max(Comparator.comparingInt(a -> a.get().msgNo));
                    if (minMsg.isPresent() && minMsg.get().isPresent()){
                        int minNo = minMsg.get().get().msgNo;
                        log.info("Removing messages expired OR read by all users up to {}", minNo);
                        Message head;
                        while((head = topic.messages.peek())!=null){
                            if (head.msgNo>minNo){
                                break;
                            }
                            topic.messages.poll();
                        }
                        //adapt 'lastMsgread' to match 'messages'
                        for (Map.Entry<User, Message> next : topic.lastMessageReadByUser.entrySet()) {
                            if (next.getValue().msgNo == minNo) {
                                topic.lastMessageReadByUser.remove(next.getKey());
                            }
                        }
                    }

                }
            }
            Thread.sleep(GARBAGE_COLLECTOR_INTERVAL_MILLISECONDS);//otherwise topics will always be busy..
        }
    }
}
