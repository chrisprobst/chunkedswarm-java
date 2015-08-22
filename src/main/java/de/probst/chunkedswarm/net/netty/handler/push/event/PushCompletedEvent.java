package de.probst.chunkedswarm.net.netty.handler.push.event;

import de.probst.chunkedswarm.net.netty.handler.push.PushTracker;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.08.15
 */
public final class PushCompletedEvent {

    private final PushTracker pushTracker;

    public PushCompletedEvent(PushTracker pushTracker) {
        Objects.requireNonNull(pushTracker);
        this.pushTracker = pushTracker;
    }

    public PushTracker getPushTracker() {
        return pushTracker;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PushCompletedEvent that = (PushCompletedEvent) o;

        return pushTracker.equals(that.pushTracker);

    }

    @Override
    public int hashCode() {
        return pushTracker.hashCode();
    }

    @Override
    public String toString() {
        return "PushCompletedEvent{" +
               "pushTracker=" + pushTracker +
               '}';
    }
}
