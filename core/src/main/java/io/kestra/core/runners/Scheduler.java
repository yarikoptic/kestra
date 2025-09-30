package io.kestra.core.runners;

import io.kestra.core.server.Service;

public interface Scheduler extends Service, Runnable {
    
    /**
     * Checks whether this scheduler is processing triggers.
     * 
     * @return {@code true} if this scheduler is active, otherwise {@code false}.
     */
    boolean isActive();
}
