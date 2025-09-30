package io.kestra.scheduler.events;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TypeTest {
    
    @Test
    void shouldGetTriggerEventType() {
        assertThat(Event.from(new ResetTrigger(null, null), TriggerEventType.class)).isEqualTo(TriggerEventType.RESET_TRIGGER);
    }
}