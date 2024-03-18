package com.example.pubsub5.handler;


import com.example.pubsub5.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.extern.slf4j.Slf4j;

import static com.example.pubsub5.PubSubApplication.*;

@Slf4j
public class WebSocketHandler implements org.springframework.web.socket.WebSocketHandler {

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("Connection established on session: {}", session.getId());
        String uri = session.getUri().toString();
        int userId = Integer.parseInt(uri.split("/")[uri.indexOf("/")]) ;
        User user;
        if (usersFromId.containsKey(userId)){
            user = usersFromId.get(userId);
            user.currentSession = session;
        }else{
            throw new ResponseStatusException(HttpStatus.NOT_ACCEPTABLE);
        }
    }

    @Override
    public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
        //irrelevant
        String tutorial = (String) message.getPayload();
        log.info("Message: {}", tutorial);
        session.sendMessage(new TextMessage("Started processing tutorial: " + session + " - " + tutorial));
        Thread.sleep(1000);
        session.sendMessage(new TextMessage("Completed processing tutorial: " + tutorial));

    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception)  {
        log.info("Exception occured: {} on session: {}", exception.getMessage(), session.getId());

    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
        log.info("Connection closed on session: {} with status: {}", session.getId(), closeStatus.getCode());
        String uri = session.getUri().toString();
        int userId = Integer.parseInt(uri.split("/")[uri.indexOf("/")]) ;
        User user;
        if (usersFromId.containsKey(userId)){
            user = usersFromId.get(userId);
            user.currentSession = null;
        }else{
            throw new ResponseStatusException(HttpStatus.NOT_ACCEPTABLE);
        }

    }

    @Override
    public boolean supportsPartialMessages() {
        return false;
    }

}
