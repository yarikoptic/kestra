package io.kestra.scheduler;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowId;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.Backfill;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.services.ConditionService;
import io.kestra.core.services.LogService;
import io.kestra.scheduler.events.CreateBackfillTrigger;
import io.kestra.scheduler.events.ResetTrigger;
import io.kestra.scheduler.events.SetDisableTrigger;
import io.kestra.scheduler.events.SetPauseBackfillTrigger;
import io.kestra.scheduler.events.TriggerCompleted;
import io.kestra.scheduler.events.TriggerCreated;
import io.kestra.scheduler.events.TriggerDeleted;
import io.kestra.scheduler.events.TriggerEvent;
import io.kestra.scheduler.events.TriggerExecuted;
import io.kestra.scheduler.events.TriggerUpdated;
import io.kestra.scheduler.internals.NextEvaluationDate;
import io.kestra.scheduler.model.TriggerState;
import io.kestra.scheduler.pubsub.TriggerExecutionPublisher;
import io.kestra.scheduler.stores.FlowMetaStore;
import io.kestra.scheduler.stores.TriggerStateStore;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * A service for handling {@link TriggerEvent}.
 */
@Singleton
public class TriggerEventHandler {
    
    private static final Logger LOG = LoggerFactory.getLogger(TriggerEventHandler.class);
    
    private final TriggerStateStore triggerStateStore;
    private final FlowMetaStore flowStateStore;
    private final TriggerExecutionPublisher triggerExecutionPublisher;
    private final LogService logService;
    private final RunContextFactory runContextFactory;
    private final ConditionService conditionService;
    
    @Inject
    public TriggerEventHandler(TriggerStateStore triggerStateStore,
                               FlowMetaStore flowStateStore,
                               TriggerExecutionPublisher triggerExecutionPublisher,
                               RunContextFactory runContextFactory,
                               ConditionService conditionService,
                               LogService logService) {
        this.triggerStateStore = triggerStateStore;
        this.flowStateStore = flowStateStore;
        this.triggerExecutionPublisher = triggerExecutionPublisher;
        this.logService = logService;
        this.conditionService = conditionService;
        this.runContextFactory = runContextFactory;
    }
    
    /**
     * Handles the given {@link TriggerEvent}.
     *
     * @param clock the scheduler clock.
     * @param vNode the trigger vNode.
     * @param event the trigger event.
     */
    public void handle(Clock clock, Integer vNode, TriggerEvent event) {
        LOG.info("Received trigger event: {}", event);
        switch (event) {
            // Events
            case TriggerCreated evt -> onTriggerCreated(evt, vNode);
            case TriggerDeleted evt -> onTriggerDeleted(evt);
            case TriggerUpdated evt -> onTriggerUpdated(clock, evt);
            case TriggerCompleted evt -> onTriggerCompleted(clock, evt);
            case TriggerExecuted evt -> onTriggerExecuted(clock, evt);
            // Commands
            case CreateBackfillTrigger evt -> onBackfill(clock, evt);
            case SetPauseBackfillTrigger evt -> onSetTriggerDisable(clock, evt);
            case SetDisableTrigger evt -> onSetTriggerDisable(clock, evt);
            case ResetTrigger evt -> onResetTrigger(clock, evt);
            default -> throw new IllegalStateException("Unexpected value: " + event);
        }
    }
    
    /**
     * Handler method for {@link CreateBackfillTrigger}.
     *
     * @param event the event.
     */
    void onBackfill(Clock clock, CreateBackfillTrigger event) {
        findTriggerState(event).ifPresent(state -> {
            state = state
                .backfill(clock, Backfill
                    .builder()
                    .start(event.backfill().start())
                    .end(event.backfill().end())
                    .inputs(event.backfill().inputs())
                    .labels(event.backfill().labels())
                    .build()
                );
            
            
            FlowWithSource flow = flowStateStore.find(FlowId.of(event.id())).orElse(null);
            if (flow == null) {
                logService.logTrigger(event.id(), Level.WARN, "Flow not found");
                return;
            }
            
            AbstractTrigger trigger = flow.getTriggers().stream()
                .filter(it -> it.getId().equals(event.id().getTriggerId()))
                .findFirst().orElse(null);
            
            if (trigger == null) {
                logService.logTrigger(event.id(), Level.WARN, "Trigger not found");
                return;
            }
            RunContext runContext = runContextFactory.of(flow, trigger);
            ConditionContext conditionContext = conditionService.conditionContext(runContext, flow, null);
            
            if (trigger instanceof PollingTriggerInterface pollingTriggerInterface) {
                ZonedDateTime nextEvaluationDate = pollingTriggerInterface.nextEvaluationDate(conditionContext, Optional.of(state.context()));
                state.updateForNextEvaluationDate(clock, nextEvaluationDate);
            }
            
            triggerStateStore.save(state);
        });
    }
    
    /**
     * Handler method for {@link SetDisableTrigger}.
     *
     * @param event the event.
     */
    void onSetTriggerDisable(Clock clock, SetPauseBackfillTrigger event) {
        findTriggerState(event).ifPresent(state -> {
            if (state.getBackfill() != null) {
                triggerStateStore.save(state.backfill(clock, state.getBackfill().toBuilder().paused(event.pause()).build()));
            }
        });
    }
    
    
    /**
     * Handler method for {@link SetDisableTrigger}.
     *
     * @param event the event.
     */
    void onSetTriggerDisable(Clock clock, SetDisableTrigger event) {
        findTriggerState(event).ifPresent(state -> {
            triggerStateStore.save(state.disabled(clock, event.disabled()));
        });
    }
    
    /**
     * Handler method for {@link TriggerCompleted}.
     *
     * @param event the event.
     */
    void onTriggerCompleted(Clock clock, TriggerCompleted event) {
        findTriggerState(event).ifPresent(state -> {
            triggerStateStore.save(state
                .locked(clock, false)
                .updateForExecutionState(clock, event.executionState())
            );
        });
    }
    
    /**
     * Handler method for {@link TriggerExecuted}.
     *
     * @param event the event.
     */
    void onTriggerExecuted(Clock clock, TriggerExecuted event) {
        findTriggerState(event).ifPresent(state -> {
            FlowWithSource flow = flowStateStore.find(FlowId.of(event.id())).orElse(null);
            if (flow == null) {
                logService.logTrigger(event.id(), Level.WARN, "Flow not found");
                return;
            }
            
            Optional<AbstractTrigger> trigger = flow.getTriggers().stream()
                .filter(it -> it.getId().equals(event.id().getTriggerId()))
                .findFirst();
            
            TriggerState newState = state;
            if (trigger.isPresent()) {
                newState = state
                    .updateForNextEvaluationDate(clock, NextEvaluationDate.get(clock, trigger.get()));
            }
            
            if (event.execution() != null) {
                newState.updateForExecution(clock, event.execution());
            }
            
            triggerStateStore.save(newState);
            
            if (event.execution() != null) {
                Execution execution = event.execution().withTenantId(state.getTenantId());
                triggerExecutionPublisher.send(execution);
            }
        });
    }
    
    /**
     * Handler method for {@link ResetTrigger}.
     *
     * @param event the event.
     */
    void onResetTrigger(Clock clock, ResetTrigger event) {
        findTriggerState(event).ifPresent(state -> {
            triggerStateStore.save(state.reset(clock));
        });
    }
    
    /**
     * Handler method for {@link TriggerUpdated}.
     *
     * @param event the event.
     */
    void onTriggerUpdated(Clock clock, TriggerUpdated event) {
        findTriggerState(event).ifPresent(state -> {
            FlowId flowId = FlowId.of(event.id().getTenantId(), event.id().getNamespace(), event.id().getFlowId(), event.revision());
            FlowWithSource flow = flowStateStore.find(flowId).orElse(null);
            if (flow == null) {
                logService.logTrigger(event.id(), Level.WARN, "Flow not found");
                return;
            }
            
            Optional<AbstractTrigger> trigger = flow.getTriggers().stream()
                .filter(it -> it.getId().equals(event.id().getTriggerId()))
                .findFirst();
            
            if (trigger.isPresent()) {
                triggerStateStore.save(state.update(clock, trigger.get()));
            } else {
                logService.logTrigger(event.id(), Level.WARN, "Trigger not found");
            }
        });
    }
    
    /**
     * Handler method for {@link TriggerDeleted}.
     *
     * @param event the event.
     */
    void onTriggerDeleted(TriggerDeleted event) {
        triggerStateStore.delete(event.id());
    }
    
    /**
     * Handler method for {@link TriggerCreated}.
     *
     * @param event the event.
     */
    void onTriggerCreated(TriggerCreated event, Integer vNode) {
        triggerStateStore.save(TriggerState.of(event.id(), event.stopAfter(), event.disabled(), vNode));
    }
    
    private Optional<TriggerState> findTriggerState(final TriggerEvent event) {
        Optional<TriggerState> state = triggerStateStore.find(event.id());
        if (state.isEmpty()) {
            logService.logTrigger(event.id(), Level.WARN, "Cannot process event. Cause: Trigger state not found.");
        }
        return state;
    }
}
