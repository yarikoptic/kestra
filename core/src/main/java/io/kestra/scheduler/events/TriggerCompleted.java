package io.kestra.scheduler.events;

import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;

/**
 * A trigger execution completed.
 */
public record TriggerCompleted(
    TriggerId id,
    String executionId,
    State.Type executionState,
    Instant timestamp
) implements TriggerEvent {
    
    public TriggerCompleted(TriggerId id, String executionId, State.Type executionState){
        this(id, executionId, executionState, Instant.now());
    }
}
