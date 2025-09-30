package io.kestra.scheduler.internals;

import io.kestra.core.utils.Disposable;
import io.kestra.scheduler.SchedulerEventQueue;
import io.kestra.scheduler.VNodesAssigner;
import io.kestra.scheduler.events.SchedulerEvent;
import io.kestra.scheduler.events.SchedulerEvent.VNodesAssignmentRelease;
import io.kestra.scheduler.events.SchedulerEvent.VNodesAssignmentRequest;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of {@link VNodesAssigner} that assigns virtual nodes (vNodes) to active scheduler services.
 */
@Singleton
public class DefaultVNodesAssigner implements VNodesAssigner {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultVNodesAssigner.class);
    
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    
    private final SchedulerEventQueue queue;
    
    private final Map<String, VNodeAssignmentSubscription> subscriptions = new ConcurrentHashMap<>();
    
    private volatile Disposable disposable;
    
    /**
     * Creates a new DefaultTriggerAssigner with the default vNode count of 16.
     */
    @Inject
    public DefaultVNodesAssigner(SchedulerEventQueue queue) {
        this.queue = Objects.requireNonNull(queue,  "queue must not be null");
    }
    
    @PostConstruct
    public void start() {
        this.disposable = queue.subscribe(event -> {
            subscriptions.values().forEach(listenerRegistration -> {
                handle(event, listenerRegistration);
            });
        });
    }
    
    /**
     * Subscribes a {@link VNodeAssignmentListener} to receive notifications about trigger
     * vNode assignment changes for the given scheduler.
     *
     * @param service the unique identifier of the scheduler service
     * @param listener  the listener that will receive assignment notifications
     */
    @Override
    public void subscribe(String service, VNodeAssignmentListener listener) {
        Objects.requireNonNull(service, "serviceId must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        
        if (stopped.get()) {
            throw new IllegalStateException("VNodeAssigner has been stopped");
        }
        
        if (subscriptions.containsKey(service)) {
            throw new IllegalStateException("Service [" + service + "] has already subscribed to vNodes (re)assignment.");
        }
        
        this.subscriptions.put(service, new VNodeAssignmentSubscription(null, service, new LoggerTriggerAssignmentListener(listener, LOG, service)));
    }
    
    private void handle(SchedulerEvent event, VNodeAssignmentSubscription registration) {
        
        final ControllerIdAndEpoch currentControllerIdAndEpoch = registration.controllerIdAndEpoch();
        
        if (!(event instanceof SchedulerEvent.VNodesAssignmentEvent vNodesAssignmentEvent) || event instanceof SchedulerEvent.VNodesAssignmentRejected ){
            return;
        }
        
        // Check whether the received event is stale
        if (currentControllerIdAndEpoch != null) {
            boolean sameController = vNodesAssignmentEvent.controllerId().equals(currentControllerIdAndEpoch.controllerId());
            boolean newerEpoch = vNodesAssignmentEvent.controllerEpoch().isAfter(currentControllerIdAndEpoch.controllerEpoch());
            /*
             * Stale if one of these conditions is true :
             * - Different controller, epoch ≤ current
             * - Same controller, epoch < current
             */
            boolean stale = (!sameController && !newerEpoch) || (sameController && vNodesAssignmentEvent.controllerEpoch().isBefore(currentControllerIdAndEpoch.controllerEpoch()));
            
            if (stale) {
                LOG.warn("Received '{}' event from out-dated controller [id={}, epoch={}]. Ignored",
                    VNodesAssignmentRelease.class.getSimpleName(),
                    vNodesAssignmentEvent.controllerId(),
                    vNodesAssignmentEvent.controllerEpoch());
                
                queue.send(new SchedulerEvent.VNodesAssignmentRejected(
                    Instant.now(),
                    vNodesAssignmentEvent.controllerId(),
                    vNodesAssignmentEvent.controllerEpoch()
                ));
                return;
            }
        }
        
        if (event instanceof VNodesAssignmentRequest request) {
            registration.listener().onVNodeAssignmentRevoked();
            // Check whether this scheduler is part of the rebalance
            if (request.schedulers().contains(registration.serviceId())) {
                subscriptions.put(
                    registration.serviceId(),
                    registration.controllerIdAndEpoch(new ControllerIdAndEpoch(request.controllerId(), request.controllerEpoch()))
                );
                queue.send(new SchedulerEvent.VNodesAssignmentReply(
                    Instant.now(),
                    request.controllerId(),
                    request.controllerEpoch(),
                    registration.serviceId()
                ));
            }
            return;
        }
        
        if (event instanceof VNodesAssignmentRelease release) {
            ControllerIdAndEpoch controllerIdAndEpoch = new ControllerIdAndEpoch(release.controllerId(), release.controllerEpoch());
            if (currentControllerIdAndEpoch == null) {
                LOG.warn("Received '{}' event from unknown controller {}. Ignored",
                    VNodesAssignmentRelease.class.getSimpleName(), controllerIdAndEpoch);
            } else if (!currentControllerIdAndEpoch.equals(controllerIdAndEpoch)) {
                LOG.warn("Received '{}' event from invalid controller. Expected {}, but was {}.",
                    VNodesAssignmentRelease.class.getSimpleName(), currentControllerIdAndEpoch, controllerIdAndEpoch);
                
                if (controllerIdAndEpoch.controllerEpoch().isAfter(currentControllerIdAndEpoch.controllerEpoch())) {
                    subscriptions.put(registration.serviceId(), registration.controllerIdAndEpoch(null));
                }
            } else {
                final Set<Integer> vnodes = release.assignments().get(registration.serviceId());
                if (vnodes != null) {
                    registration.listener().onVNodeAssignmentAssigned(vnodes);
                } else {
                    LOG.warn("Received '{}' from controller {} with no vNode assignment. This may indicate that this scheduler failed to revoke previously assigned vnodes before the rebalancing timeout.",
                        VNodesAssignmentRelease.class.getSimpleName(), currentControllerIdAndEpoch);
                }
                subscriptions.put(registration.serviceId(), registration.controllerIdAndEpoch(null));
            }
        }
    }
    
    /**
     * Stops.
     * <p>
     * This method is idempotent and safe to call multiple times.
     */
    @PreDestroy
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return; // Already stopped
        }
        
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }
    
    /**
     * Represents a subscription for VNode assignments.
     *
     * @param controllerIdAndEpoch the controller metadata.
     * @param serviceId          the service identifier.
     * @param listener             the vNode rebalance listener.
     */
    private record VNodeAssignmentSubscription(
        ControllerIdAndEpoch controllerIdAndEpoch,
        String serviceId, 
        VNodeAssignmentListener listener
    ) {
        public VNodeAssignmentSubscription controllerIdAndEpoch(ControllerIdAndEpoch controllerIdAndEpoch) {
            return new VNodeAssignmentSubscription(controllerIdAndEpoch, serviceId, listener);
        }
    }
    
    /**
     * Represents a VNode controller generation.
     *
     * @param controllerId    the controller identifier.
     * @param controllerEpoch the controller epoch.
     */
    private record ControllerIdAndEpoch(String controllerId, Instant controllerEpoch){
        
        @Override
        public String toString() {
            return "[controllerId=" + controllerId + ", controllerEpoch=" + controllerEpoch + "]";
        }
    }
}
