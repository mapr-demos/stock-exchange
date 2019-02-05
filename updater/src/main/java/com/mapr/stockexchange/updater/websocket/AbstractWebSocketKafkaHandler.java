package com.mapr.stockexchange.updater.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.stockexchange.updater.websocket.entity.SubscriptionMessageType;
import com.mapr.stockexchange.updater.websocket.entity.WebSocketSubscriptionRequest;
import com.mapr.stockexchange.updater.websocket.entity.WebSocketSubscriptionResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import reactor.core.Disposable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractWebSocketKafkaHandler extends AbstractWebSocketHandler {

    private final Map<WebSocketSession, WebSocketKafkaSubscriptions> connections = new ConcurrentHashMap<>();
    final ObjectMapper mapper = new ObjectMapper();

    public int getSubscribersAmount() {
        return connections.size();
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        connections.put(session, new WebSocketKafkaSubscriptions());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        WebSocketKafkaSubscriptions subscription = connections.get(session);

        if(subscription == null)
            return;

        subscription.unsubscribeFromAll();
        connections.remove(session);
        onSubscribersAmountChange();
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        log.info("Received from {} message {}", session.getId(), message.getPayload());
        handleStringMessage(session, message.getPayload());
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
        log.info("Received from {} message {}", session.getId(), message.toString());
        handleStringMessage(session, message.toString());
    }

    protected void onSubscribersAmountChange() {
        log.info("Current amount of subscribers: {}", connections.size());
    }

    @SneakyThrows
    private void handleStringMessage(WebSocketSession session, String message) {
        log.info("Received {}", message);
        WebSocketSubscriptionRequest msg = mapper.readValue(message, WebSocketSubscriptionRequest.class);
        handleMessage(session, msg);
    }

    private void handleMessage(WebSocketSession session, WebSocketSubscriptionRequest message) {
        switch (message.getType()) {
            case SUBSCRIBE:
                subscribe(session, message.getTopic(), subscribe(session, message.getTopic()));
                break;
            case UNSUBSCRIBE:
                unsubscribe(session, message.getTopic());
                break;
        }
    }

    @SneakyThrows
    private void sendMessage(WebSocketSession session, WebSocketSubscriptionResponse response) {
        String message = mapper.writeValueAsString(response);
        sendMessage(session, message);
    }

    @SneakyThrows
    void sendMessage(WebSocketSession session, Object object) {
        String msg = mapper.writeValueAsString(object);
        sendMessage(session, msg);
    }

    void sendMessage(WebSocketSession session, String message) {
        log.debug("Sending {} to {}", message, session.getId());
        WebSocketMessage<?> msg = new TextMessage(message);
        sendMessage(session, msg);
    }

    @SneakyThrows
    void sendMessage(WebSocketSession session, byte[] message) {
        Object msg = mapper.readValue(message, Object.class);
        sendMessage(session, msg);
    }

    @SneakyThrows
    void sendMessage(WebSocketSession session, WebSocketMessage<?> message) {
        session.sendMessage(message);
    }

    abstract Disposable subscribe(WebSocketSession session, String topic);

    void subscribe(WebSocketSession session, String topic, Disposable Disposable) {
        connections.get(session).subscribe(topic, Disposable);

        WebSocketSubscriptionResponse msg = new WebSocketSubscriptionResponse();
        msg.setStatus(SubscriptionMessageType.SUBSCRIBE);
        msg.setTopicName(topic);

        sendMessage(session, msg);
        onSubscribersAmountChange();
    }

    void unsubscribe(WebSocketSession session, String topic) {
        connections.get(session).unsubscribe(topic);

        WebSocketSubscriptionResponse msg = new WebSocketSubscriptionResponse();
        msg.setStatus(SubscriptionMessageType.UNSUBSCRIBE);
        msg.setTopicName(topic);

        sendMessage(session, msg);
        onSubscribersAmountChange();
    }
}
