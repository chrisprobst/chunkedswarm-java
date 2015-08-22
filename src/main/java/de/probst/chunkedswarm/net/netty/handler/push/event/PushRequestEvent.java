package de.probst.chunkedswarm.net.netty.handler.push.event;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 18.08.15
 */
public final class PushRequestEvent {

    private final ByteBuffer payload;
    private final int sequence;
    private final int priority;
    private final Duration duration;

    public PushRequestEvent(ByteBuffer payload, int sequence, int priority, Duration duration) {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(duration);
        this.payload = payload;
        this.sequence = sequence;
        this.priority = priority;
        this.duration = duration;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    public int getSequence() {
        return sequence;
    }

    public int getPriority() {
        return priority;
    }

    public Duration getDuration() {
        return duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PushRequestEvent pushEvent = (PushRequestEvent) o;

        if (sequence != pushEvent.sequence) return false;
        if (priority != pushEvent.priority) return false;
        if (!payload.equals(pushEvent.payload)) return false;
        return duration.equals(pushEvent.duration);

    }

    @Override
    public int hashCode() {
        int result = payload.hashCode();
        result = 31 * result + sequence;
        result = 31 * result + priority;
        result = 31 * result + duration.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PushEvent{" +
               "payload=" + payload +
               ", sequence=" + sequence +
               ", priority=" + priority +
               ", duration=" + duration +
               '}';
    }
}
