package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmId;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class SwarmIdAquisitionEvent {

    private final SwarmId swarmId;

    public SwarmIdAquisitionEvent(SwarmId swarmId) {
        Objects.requireNonNull(swarmId);
        this.swarmId = swarmId;
    }

    public SwarmId getSwarmId() {
        return swarmId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIdAquisitionEvent that = (SwarmIdAquisitionEvent) o;

        return swarmId.equals(that.swarmId);
    }

    @Override
    public int hashCode() {
        return swarmId.hashCode();
    }

    @Override
    public String toString() {
        return "SwarmIdAquisitionEvent{" +
               "swarmId=" + swarmId +
               '}';
    }
}
