package com.example.pubsub5;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;


@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
@RestController
@Import(WebSocketServer.class)
public class PubSub5Application {
	public static class User {
		public String name;
		public int id;
		public WebSocketSession currentSession ;

		public User(String name, int id) {
			this.name = name;
			this.id = id;
			this.currentSession = null;
		}
	}

	public static class Msg {
		private static int _counter = 0;
		public static synchronized int increment_counter(){
			return _counter++;
		}
		public String content;
		public int msgNo;
		public Timestamp timestamp;
		public Msg(String content) {
			this.content = content;
			this.msgNo = increment_counter();
			this.timestamp = new Timestamp(System.currentTimeMillis());
		}
	}


	public static final int MESSAGE_TTL_MILLISECONDS = 30000;
	public static final int NB_THREADS = 8;
	public static final int GARBAGE_COLLECTOR_TIMEOUT = 5000;
	public static final int NB_SOCKETS = 1000;//TODO: add websockets dynamically per user registration
	public static ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<Integer, User> users = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, User> usernames = new ConcurrentHashMap<>();


	static class Dispatch implements Callable<Boolean> {
		private final User user;
		public Dispatch(User user){
			this.user = user;
		}
		@Override
		public Boolean call() throws IOException {
			for (Topic topic:topics.values()){
				if (topic.subscribed.containsKey(user)){
					Iterator<Msg> it = topic.UnreadMessagesIterator(user);
					while(it.hasNext()){
						Msg m = it.next();
						user.currentSession.sendMessage(new TextMessage(format(topic, m)));
						topic.lastMsgRead.put(user, m);
					}
				}
			}
			return true;
		}
	}

	static class GarbageCollectorInfiniteLoop implements Callable<Boolean> {
		public GarbageCollectorInfiniteLoop(){}
		@Override
		public Boolean call() throws InterruptedException {
			while (true){
				log.info("Attempting to clean messages");
				for (Topic topic:topics.values()){
					Msg head;
					synchronized (topic){
						Timestamp now = new Timestamp(System.currentTimeMillis());

						Optional<Msg> minMsg1 = topic.messages.stream().filter(m->(now.getTime()-m.timestamp.getTime()>MESSAGE_TTL_MILLISECONDS)).reduce((first, second)->second);
						Optional<Msg> minMsg2 = Optional.empty();
						if (topic.subscribed.keySet().size()==topic.lastMsgRead.keySet().size()) {
							minMsg2 = topic.lastMsgRead.values().stream().min(Comparator.comparingInt(a -> a.msgNo));
						}
						Optional<Optional<Msg>> minMsg = Stream.of(minMsg1, minMsg2).filter(m->m.isPresent()).max(Comparator.comparingInt(a -> a.get().msgNo));
						if (minMsg.isPresent() && minMsg.get().isPresent()){
							int minNo = minMsg.get().get().msgNo;
							log.info("Removing messages expired OR read by all users up to {}", minNo);
							while((head = topic.messages.peek())!=null){
								if (head.msgNo>minNo){
									break;
								}
								topic.messages.poll();
							}
							//adapt 'lastMsgread' to match 'messages'
                            for (Map.Entry<User, Msg> next : topic.lastMsgRead.entrySet()) {
                                if (next.getValue().msgNo == minNo) {
                                    topic.lastMsgRead.remove(next.getKey());
                                }
                            }
						}

					}
				}
				Thread.sleep(GARBAGE_COLLECTOR_TIMEOUT);//otherwise topics will always be busy..
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(PubSub5Application.class, args);

		// Main Dispatcher + Garbage Collector Loop
		ScheduledExecutorService scheduler =
				Executors.newScheduledThreadPool(NB_THREADS);
		scheduler.submit(new GarbageCollectorInfiniteLoop());
		Collection<Callable<Boolean>> callables;
		while (true){
			callables = new ArrayList<>();
			for (User user:users.values()){
				if (user.currentSession!=null){
					callables.add(new Dispatch(user));
				}
			}
			scheduler.invokeAll(callables);
		}

	}
	public static String format(Topic topic, Msg m){
		return "\n\n\nTopic: " + topic.name + "\nTime: " + new Timestamp(System.currentTimeMillis()) + "\nMessage: " + m.content;
	}




	// HTTP Endpoints:
	@GetMapping("/hello")
	public String sayHello(@RequestParam(value = "myName", defaultValue = "World") String name) {
		return String.format("Hello %s!", name);
	}

	@PostMapping("/addUser")
	public void addUser(@RequestParam(value = "userName") String userName){
		if (users.size()>=NB_SOCKETS-1){
			throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE);
		}else{
			int id = users.size();
			User u = new User(userName, id);
			users.put(id, u);
			usernames.put(userName, u);
		}
		log.info("Added username {}.", userName);
	}

	@PostMapping("/addTopic")
	public void addTopic(@RequestParam(value = "topicName") String topicName){
		topics.put(topicName, new Topic(topicName));
		log.info("Added topic {}.", topicName);
	}

	@PostMapping("/subscribe")
	public void subscribe(@RequestParam(value = "userName") String userName, @RequestParam(value = "topicName") String topicName) {
		try {
			Topic topic;
			User user;
			topic = topics.get(topicName);
			user = usernames.get(userName);
			topic.subscribe(user);
		} catch(NullPointerException ex) {
			System.out.println(ex.getMessage());
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
		log.info("User {} subscribed to topic {}.", userName, topicName);
	}

	@PostMapping("/unsubscribe")
	public void unsubscribe(@RequestParam(value = "userName") String userName, @RequestParam(value = "topicName") String topicName) {
		try {
			Topic topic;
			User user;
			topic = topics.get(topicName);
			user = usernames.get(userName);
			topic.unsubscribe(user);
		} catch(NullPointerException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
		log.info("User {} unsubscribed to topic {}.", userName, topicName);
	}

	@PostMapping("/publish")
	public void publish(@RequestParam(value = "userName") String userName,
						@RequestParam(value = "topicName") String topicName,
						@RequestParam(value = "msg") String msg) {
		try {
			Topic topic;
			topic = topics.get(topicName);
			topic.publish(new Msg(msg));
		} catch(NullPointerException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
		log.info("User {} published to topic {} the following message {}.", userName, topicName, msg);
	}
}


