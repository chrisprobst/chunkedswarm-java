package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmID;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class SwarmIDCollectionEvent {

    private final Set<SwarmID> swarmIDs;
    private final UpdateNeighboursMessage updateNeighboursMessage;

    public SwarmIDCollectionEvent(Set<SwarmID> swarmIDs, UpdateNeighboursMessage updateNeighboursMessage) {
        Objects.requireNonNull(swarmIDs);
        Objects.requireNonNull(updateNeighboursMessage);
        this.swarmIDs = new HashSet<>(swarmIDs);
        this.updateNeighboursMessage = updateNeighboursMessage;
    }

    public Set<SwarmID> getSwarmIDs() {
        return swarmIDs;
    }

    public UpdateNeighboursMessage getUpdateNeighboursMessage() {
        return updateNeighboursMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIDCollectionEvent that = (SwarmIDCollectionEvent) o;

        if (!swarmIDs.equals(that.swarmIDs)) return false;
        return updateNeighboursMessage.equals(that.updateNeighboursMessage);
    }

    @Override
    public int hashCode() {
        int result = swarmIDs.hashCode();
        result = 31 * result + updateNeighboursMessage.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmIDCollectionEvent{" +
               "swarmIDs=" + swarmIDs +
               ", updateNeighboursMessage=" + updateNeighboursMessage +
               '}';
    }
}
