package com.mapr.stockexchange.updater.websocket;

import reactor.core.Disposable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class WebSocketKafkaSubscriptions {

    private final Map<String, Disposable> subscriptions = new ConcurrentHashMap<>();

    void subscribe(String topic, Disposable disposable) {
        Disposable currentDisposable = subscriptions.get(topic);

        if(currentDisposable != null)
            currentDisposable.dispose();

        subscriptions.put(topic, disposable);
    }

    void unsubscribe(String topic) {
        Disposable disposable = subscriptions.get(topic);

        if(disposable != null)
            disposable.dispose();

        subscriptions.remove(topic);
    }

    void unsubscribeFromAll() {
        subscriptions.values().forEach(Disposable::dispose);
    }

}
