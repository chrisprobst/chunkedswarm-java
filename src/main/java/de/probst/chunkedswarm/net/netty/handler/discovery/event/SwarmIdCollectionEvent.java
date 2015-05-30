package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmId;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class SwarmIdCollectionEvent {

    private final Set<SwarmId> swarmIds;
    private final UpdateNeighboursMessage updateNeighboursMessage;

    public SwarmIdCollectionEvent(Set<SwarmId> swarmIds, UpdateNeighboursMessage updateNeighboursMessage) {
        Objects.requireNonNull(swarmIds);
        Objects.requireNonNull(updateNeighboursMessage);
        this.swarmIds = new HashSet<>(swarmIds);
        this.updateNeighboursMessage = updateNeighboursMessage;

    }

    public Set<SwarmId> getSwarmIds() {
        return swarmIds;
    }

    public UpdateNeighboursMessage getUpdateNeighboursMessage() {
        return updateNeighboursMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIdCollectionEvent that = (SwarmIdCollectionEvent) o;

        if (!swarmIds.equals(that.swarmIds)) return false;
        return updateNeighboursMessage.equals(that.updateNeighboursMessage);
    }

    @Override
    public int hashCode() {
        int result = swarmIds.hashCode();
        result = 31 * result + updateNeighboursMessage.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmIdCollectionEvent{" +
               "swarmIds=" + swarmIds +
               ", updateNeighboursMessage=" + updateNeighboursMessage +
               '}';
    }
}
