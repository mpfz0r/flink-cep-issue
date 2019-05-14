package org.classdump.alerts;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class Event implements Timestamped, Comparable<Event>  {
    public final long timestamp;
    protected final Map<String, Object> props;


    public Event(long timestamp) {
        this.timestamp = timestamp;
        props = Maps.newHashMap();
    }

    public static Event event(long timestamp) {
        return new Event(timestamp);
    }

    public long timestamp() {
        return timestamp;
    }

    public boolean canEquals(Object o) {
        return o instanceof Event;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return timestamp == event.timestamp &&
                Objects.equals(props, event.props);
    }

    public int compareTo(Event other) {
        if (other == null)
            return 1;

        return Long.compare(this.timestamp(), other.timestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(props, timestamp);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Event.class.getSimpleName() + "[", "]")
                .add("props=" + props)
                .add("timestamp=" + timestamp)
                .toString();
    }

    public String getUsername() {
        return props.get("username").toString();
    }

    public String getServiceName() {
        return props.get("servicename").toString();
    }

    public Object get(String key) {
        return props.get(key);
    }

    public Event prop(String key, Object value) {
        props.put(key, value);
        return this;
    }
}
