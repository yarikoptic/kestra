package io.kestra.scheduler;

import io.kestra.core.utils.Disposable;
import io.kestra.scheduler.events.SchedulerEvent;

import java.io.Closeable;
import java.util.function.Consumer;

public interface SchedulerEventQueue extends Closeable {
    
    /**
     * Publishes a {@link SchedulerEvent} event.
     * 
     * @param event the event to be published.
     */
    void send(SchedulerEvent event);
    
    Disposable subscribe(Consumer<SchedulerEvent> handler);
}
