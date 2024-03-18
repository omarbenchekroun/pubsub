package com.example.pubsub5;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@ContextConfiguration(classes= PubSubApplication.class)
@SpringBootTest
class PubSubApplicationTests {
	static final int NB_THREADS = 8;
	void post(String url, HashMap<String, Object> params){
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url);
		for (Map.Entry<String, Object> entry : params.entrySet()){
			builder = builder.queryParam(entry.getKey(), entry.getValue());
		}
		HttpEntity<?> entity = new HttpEntity<>(headers);
		restTemplate.exchange(
				builder.toUriString(),
				HttpMethod.POST,
				entity,
				String.class);
	}

	class addAndSubscribe implements Callable<Boolean> {
		private final int id;
		public addAndSubscribe(int id){
			this.id = id;
		}
		@Override
		public Boolean call(){
			post("http://localhost:8080/user", new HashMap<>(){{
				put("userName", String.format("Sardukar%d", id));
			}});
			post("http://localhost:8080/subscribe", new HashMap<>(){{
				put("userName", String.format("Sardukar%d", id));
				put("topicName", "DUNE");
			}});
			return true;
		}
	}
	class publish implements Callable<Boolean> {
		private final int id;
		public publish(int id){
			this.id = id;
		}
		@Override
		public Boolean call(){
			post("http://localhost:8080/publish", new HashMap<>(){{
				put("userName", "Paul");
				put("topicName", "DUNE");
				put("msg", String.format("Calling my loyal Sardukar no%d", id));
			}});
			return true;
		}
	}
	@Test
	void SubscribersTest() throws InterruptedException {
		final int NB_CONCURRENT_SUSCRIBERS = 500;

		post("http://localhost:8080/topic", new HashMap<>(){{
			put("topicName", "DUNE");
		}});
		post("http://localhost:8080/user", new HashMap<>(){{
			put("userName", "Paul");
		}});
		post("http://localhost:8080/subscribe", new HashMap<>(){{
			put("userName", "Paul");
			put("topicName", "DUNE");
		}});
		ExecutorService executor = Executors.newFixedThreadPool(NB_THREADS);
		Collection<Callable<Boolean>> a = new ArrayList<>();
		for (int i = 0; i<NB_CONCURRENT_SUSCRIBERS; i++){
			a.add(new addAndSubscribe(i));
		}
		executor.invokeAll(a);
		post("http://localhost:8080/publish", new HashMap<>(){{
			put("userName", "Paul");
			put("topicName", "DUNE");
			put("msg", "Hello Guys whats up? Anyway Arrakis is being attacked and all.. Halp.");
		}});
	}

	@Test
	void PublicationsTest() throws InterruptedException {
		final int NB_CONCURRENT_PUBLICATIONS = 10000;

		ExecutorService executor = Executors.newFixedThreadPool(NB_THREADS);
		Collection<Callable<Boolean>> b = new ArrayList<>();
		for (int i=0; i<NB_CONCURRENT_PUBLICATIONS; i++){
			b.add(new publish(i));
		}
		executor.invokeAll(b);
	}

}
