package de.probst.chunkedswarm.net.netty.handler.forwarding.event;

import de.probst.chunkedswarm.net.netty.handler.forwarding.ForwardingTracker;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.08.15
 */
public final class ForwardingCompletedEvent {

    private final ForwardingTracker forwardingTracker;

    public ForwardingCompletedEvent(ForwardingTracker forwardingTracker) {
        Objects.requireNonNull(forwardingTracker);
        this.forwardingTracker = forwardingTracker;
    }

    public ForwardingTracker getForwardingTracker() {
        return forwardingTracker;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ForwardingCompletedEvent that = (ForwardingCompletedEvent) o;

        return forwardingTracker.equals(that.forwardingTracker);

    }

    @Override
    public int hashCode() {
        return forwardingTracker.hashCode();
    }

    @Override
    public String toString() {
        return "ForwardingCompletedEvent{" +
               "forwardingTracker=" + forwardingTracker +
               '}';
    }
}
