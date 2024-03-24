package com.example.pubsub5.config;

import com.example.pubsub5.handler.MyWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import static com.example.pubsub5.PubSubApplication.NB_SOCKETS;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        for (int counter=0; counter<NB_SOCKETS; counter++){
            registry.addHandler(tutorialHandler(), String.format("%d",counter))
                    .setAllowedOrigins("*");
        }
    }

    @Bean
    WebSocketHandler tutorialHandler() {
        return new MyWebSocketHandler();
    }
}
