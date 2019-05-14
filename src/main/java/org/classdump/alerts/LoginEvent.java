package org.classdump.alerts;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class LoginEvent extends Event {

    public LoginEvent(long timestamp) {
        super(timestamp);
    }
    public static LoginEvent event(long timestamp) {
        return new LoginEvent(timestamp);
    }

    @Override
    public boolean canEquals(Object o) {
        return o instanceof LoginEvent;
    }

}
