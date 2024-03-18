package com.example.pubsub5;

import com.example.pubsub5.callables.DispatchTopicsToUser;
import com.example.pubsub5.callables.GarbageCollectorInfiniteLoop;
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

import java.util.*;
import java.util.concurrent.*;

@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
@RestController
@Import(WebSocketServer.class)
public class PubSubApplication {
	public static final int MESSAGE_TTL_MILLISECONDS = 30000;
	public static final int NB_THREADS = 8;
	public static final int GARBAGE_COLLECTOR_INTERVAL_MILLISECONDS = 5000;
	public static final int NB_SOCKETS = 1000;//TODO: add websockets dynamically per user registration
	public static ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<Integer, User> usersFromId = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, User> usersFromName = new ConcurrentHashMap<>();


	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(PubSubApplication.class, args);

		// Main Dispatcher + Garbage Collector Loop
		ScheduledExecutorService scheduler =
				Executors.newScheduledThreadPool(NB_THREADS);
		scheduler.submit(new GarbageCollectorInfiniteLoop(topics));
		Collection<Callable<Boolean>> callables;
		while (true){
			callables = new ArrayList<>();
			for (User user: usersFromName.values()){
				if (user.currentSession!=null){
					callables.add(new DispatchTopicsToUser(topics, user));
				}
			}
			scheduler.invokeAll(callables);
		}

	}


	// HTTP Endpoints:
	@GetMapping("/hello")
	public String sayHello(@RequestParam(value = "myName", defaultValue = "World") String name) {
		return String.format("Hello %s!", name);
	}

	@PostMapping("/user")
	public void addUser(@RequestParam(value = "userName") String userName){
		if (usersFromId.size()>=NB_SOCKETS-1){
			throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE);
		}else{
			User u = new User(userName);
			usersFromId.put(u.id, u);
			usersFromName.put(userName, u);
			log.info("Added username {} with following allocated websocket {}.", userName, u.getWebSocketURL());
		}
	}

	@GetMapping("/websocketURL")
	public String getWebSocketURL(@RequestParam(value = "userName") String userName) {
		return usersFromName.get(userName).getWebSocketURL();
	}

	@PostMapping("/topic")
	public void addTopic(@RequestParam(value = "topicName") String topicName){
		topics.put(topicName, new Topic(topicName));
		log.info("Added topic {}.", topicName);
	}

	@PostMapping("/subscribe")
	public void subscribe(@RequestParam(value = "userName") String userName, @RequestParam(value = "topicName") String topicName) {
		try {
			Topic topic = topics.get(topicName);
			User user = usersFromName.get(userName);
			topic.subscribe(user);
			log.info("User {} subscribed to topic {}.", userName, topicName);
		} catch(NullPointerException ex) {
			System.out.println(ex.getMessage());
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
	}

	@PostMapping("/unsubscribe")
	public void unsubscribe(@RequestParam(value = "userName") String userName, @RequestParam(value = "topicName") String topicName) {
		try {
			Topic topic = topics.get(topicName);
			User user = usersFromName.get(userName);
			topic.unsubscribe(user);
			log.info("User {} unsubscribed to topic {}.", userName, topicName);
		} catch(NullPointerException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
	}

	@PostMapping("/publish")
	public void publish(@RequestParam(value = "userName") String userName,
						@RequestParam(value = "topicName") String topicName,
						@RequestParam(value = "msg") String msg) {
		try {
			Topic topic = topics.get(topicName);
			topic.publish(new Message(msg));
			log.info("User {} published to topic {} the following message {}.", userName, topicName, msg);
		} catch(NullPointerException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND);
		}
	}
}


