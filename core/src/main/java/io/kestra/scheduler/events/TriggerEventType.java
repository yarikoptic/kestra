package io.kestra.scheduler.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.kestra.core.utils.Enums;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Supported event or command types for trigger.
 */
public enum TriggerEventType {
    // EVENTS
    TRIGGER_CREATED,
    TRIGGER_UPDATED,
    TRIGGER_DELETED,
    TRIGGER_EXECUTED,
    TRIGGER_COMPLETED,
    // COMMANDS,
    BACKFILL_TRIGGER,
    RESET_TRIGGER,
    DISABLE_TRIGGER,
    // ERROR
    INVALID;
    
    @JsonCreator
    static TriggerEventType from(final String s) {
        return Enums.getForNameIgnoreCase(s, TriggerEventType.class, INVALID);
    }
}
