package io.kestra.scheduler.events;

import io.kestra.core.utils.Enums;

import java.util.Locale;
import java.util.regex.Pattern;

public interface Event {
    
    Pattern NORMALIZE = Pattern.compile("([a-z])([A-Z])");
    
    static <T extends Enum<T>> T from(final Object o, final Class<T> enumType) {
        String name = NORMALIZE.matcher(o.getClass().getSimpleName()).replaceAll("$1_$2").toUpperCase(Locale.ROOT);
        return Enums.getForNameIgnoreCase(name, enumType);
    }
}
