package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmID;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class SwarmIDAcquisitionEvent {

    private final SwarmID swarmID;

    public SwarmIDAcquisitionEvent(SwarmID swarmID) {
        Objects.requireNonNull(swarmID);
        this.swarmID = swarmID;
    }

    public SwarmID getSwarmID() {
        return swarmID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIDAcquisitionEvent that = (SwarmIDAcquisitionEvent) o;

        return swarmID.equals(that.swarmID);
    }

    @Override
    public int hashCode() {
        return swarmID.hashCode();
    }

    @Override
    public String toString() {
        return "SwarmIDAcquisitionEvent{" +
               "swarmID=" + swarmID +
               '}';
    }
}
