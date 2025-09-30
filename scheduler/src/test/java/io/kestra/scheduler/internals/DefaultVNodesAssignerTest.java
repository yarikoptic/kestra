package io.kestra.scheduler.internals;

import io.kestra.scheduler.events.SchedulerEvent;
import io.kestra.scheduler.events.SchedulerEvent.VNodesAssignmentRejected;
import io.kestra.scheduler.events.SchedulerEvent.VNodesAssignmentRelease;
import io.kestra.scheduler.events.SchedulerEvent.VNodesAssignmentReply;
import io.kestra.scheduler.events.SchedulerEvent.VNodesAssignmentRequest;
import io.kestra.scheduler.utils.InMemorySchedulerEventQueue;
import io.kestra.scheduler.utils.TestVNodeAssignmentListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThatList;

class DefaultVNodesAssignerTest {
    
    private static final String TEST_SCHEDULER_1 = "scheduler-1";
    private static final String TEST_CONTROLLER_1 = "controller-1";
    
    private InMemorySchedulerEventQueue queue;
    private DefaultVNodesAssigner assigner;
    
    @BeforeEach
    void setUp() {
        queue = new InMemorySchedulerEventQueue();
        assigner = new DefaultVNodesAssigner(queue);
    }
    
    @Test
    void shouldSubscribeWhenServiceAndListenerAreValidGivenActiveAssigner() {
        // GIVEN
        TestVNodeAssignmentListener listener = new TestVNodeAssignmentListener();
        
        // WHEN
        assigner.subscribe(TEST_SCHEDULER_1, listener);
        
        // THEN
        assertThatList(listener.assigned()).isEmpty();
        assertThatList(queue.sentEvents()).isEmpty();
    }
    
    @Test
    void shouldThrowWhenSubscribingTwiceGivenSameService() {
        // GIVEN
        TestVNodeAssignmentListener listener = new TestVNodeAssignmentListener();
        assigner.subscribe(TEST_SCHEDULER_1, listener);
        
        // WHEN / THEN
        assertThatThrownBy(() -> assigner.subscribe(TEST_SCHEDULER_1, listener))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already subscribed");
    }
    
    @Test
    void shouldThrowWhenSubscribeGivenStoppedAssigner() {
        // GIVEN
        assigner.stop();
        TestVNodeAssignmentListener listener = new TestVNodeAssignmentListener();
        
        // WHEN / THEN
        assertThatThrownBy(() -> assigner.subscribe(TEST_SCHEDULER_1, listener))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("stopped");
    }
    
    @Test
    void shouldSendAssignmentReplyWhenReceivingRequestGivenSubscribedService() {
        // GIVEN
        TestVNodeAssignmentListener listener = new TestVNodeAssignmentListener();
        assigner.subscribe(TEST_SCHEDULER_1, listener);
        assigner.start();
        
        Instant epoch = Instant.now();
        VNodesAssignmentRequest request = new VNodesAssignmentRequest(
            Instant.now(), TEST_CONTROLLER_1, epoch, Set.of(TEST_SCHEDULER_1)
        );
        
        // WHEN
        queue.send(request);
        
        // THEN
        List<SchedulerEvent> sent = queue.sentEvents();
        assertThatList(sent).hasSize(2);
        
        SchedulerEvent last = sent.getLast();
        assertThat(last).isInstanceOf(VNodesAssignmentReply.class);
        assertThat(((VNodesAssignmentReply)last).controllerEpoch()).isEqualTo(epoch);
        assertThat(((VNodesAssignmentReply)last).controllerId()).isEqualTo(TEST_CONTROLLER_1);
        assertThat(((VNodesAssignmentReply)last).schedulerId()).isEqualTo(TEST_SCHEDULER_1);
        assertThat(((VNodesAssignmentReply)last).controllerEpoch()).isEqualTo(epoch);
    }
    
    @Test
    void shouldIgnoreStaleEventWhenEpochOlderGivenExistingControllerEpoch() {
        // GIVEN
        TestVNodeAssignmentListener listener = new TestVNodeAssignmentListener();
        assigner.subscribe(TEST_SCHEDULER_1, listener);
        assigner.start();
        
        Instant epoch = Instant.now();
        VNodesAssignmentRequest assignmentRequest = new VNodesAssignmentRequest(
            Instant.now(), TEST_CONTROLLER_1, epoch, Set.of(TEST_SCHEDULER_1)
        );
        queue.send(assignmentRequest);
        
        // WHEN
        Instant oldControllerEpoch = epoch.minusSeconds(100);
        VNodesAssignmentRelease release = new VNodesAssignmentRelease(
            Instant.now(), TEST_CONTROLLER_1, oldControllerEpoch, Map.of(TEST_SCHEDULER_1, Set.of(1, 2))
        );
        queue.send(release);
        
        // THEN
        List<SchedulerEvent> sent = queue.sentEvents();
        assertThatList(sent).hasSize(4);
        SchedulerEvent last = sent.getLast();
        assertThat(last).isInstanceOf(VNodesAssignmentRejected.class);
        assertThat(((VNodesAssignmentRejected)last).controllerEpoch()).isEqualTo(oldControllerEpoch);
        assertThat(((VNodesAssignmentRejected)last).controllerId()).isEqualTo(TEST_CONTROLLER_1);
        assertThat(((VNodesAssignmentRejected)last).controllerEpoch()).isEqualTo(oldControllerEpoch);
    }
}