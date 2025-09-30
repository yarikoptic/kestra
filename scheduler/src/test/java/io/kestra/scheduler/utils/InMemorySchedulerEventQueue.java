package io.kestra.scheduler.utils;

import io.kestra.core.utils.Disposable;
import io.kestra.scheduler.SchedulerEventQueue;
import io.kestra.scheduler.events.SchedulerEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Simple in-memory event queue implementation.
 */
public class InMemorySchedulerEventQueue implements SchedulerEventQueue {
    private final List<Consumer<SchedulerEvent>> subscribers = new CopyOnWriteArrayList<>();
    private final List<SchedulerEvent> sentEvents = new CopyOnWriteArrayList<>();
    private volatile boolean closed = false;
    
    @Override
    public void send(SchedulerEvent event) {
        sentEvents.add(event);
        for (Consumer<SchedulerEvent> subscriber : subscribers) {
            subscriber.accept(event);
        }
    }
    
    @Override
    public Disposable subscribe(Consumer<SchedulerEvent> handler) {
        subscribers.add(handler);
        return Disposable.of(() -> subscribers.remove(handler));
    }
    
    public List<SchedulerEvent> sentEvents() {
        return sentEvents;
    }
    
    public List<Consumer<SchedulerEvent>> subscribers() {
        return subscribers;
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    @Override
    public void close() {
        closed = true;
        subscribers.clear();
    }
}
