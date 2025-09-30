package io.kestra.scheduler;

import io.kestra.core.utils.Disposable;
import io.kestra.scheduler.events.TriggerEvent;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A service interface for publishing and subscribing to the {@link TriggerEvent} queue.
 */
public interface TriggerEventQueue extends Closeable {
    
    void send(TriggerEvent triggerEvent);
    
    Disposable subscribe(Set<Integer> vNodes, BiConsumer<Integer, List<TriggerEvent>> handler);
    
}
