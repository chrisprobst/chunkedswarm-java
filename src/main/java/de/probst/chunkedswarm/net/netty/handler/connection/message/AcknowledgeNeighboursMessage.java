package de.probst.chunkedswarm.net.netty.handler.connection.message;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class AcknowledgeNeighboursMessage implements Serializable {

    private final Set<UUID> addedOutboundNeighbours;
    private final Set<UUID> removedOutboundNeighbours;
    private final Set<UUID> addedInboundNeighbours;
    private final Set<UUID> removedInboundNeighbours;

    public AcknowledgeNeighboursMessage() {
        this(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>());
    }

    public AcknowledgeNeighboursMessage(Set<UUID> addedOutboundNeighbours,
                                        Set<UUID> removedOutboundNeighbours,
                                        Set<UUID> addedInboundNeighbours,
                                        Set<UUID> removedInboundNeighbours) {
        Objects.requireNonNull(addedOutboundNeighbours);
        Objects.requireNonNull(removedOutboundNeighbours);
        Objects.requireNonNull(addedInboundNeighbours);
        Objects.requireNonNull(removedInboundNeighbours);
        this.addedOutboundNeighbours = addedOutboundNeighbours;
        this.removedOutboundNeighbours = removedOutboundNeighbours;
        this.addedInboundNeighbours = addedInboundNeighbours;
        this.removedInboundNeighbours = removedInboundNeighbours;
    }

    public boolean isEmpty() {
        return addedOutboundNeighbours.isEmpty() &&
               removedOutboundNeighbours.isEmpty() &&
               addedInboundNeighbours.isEmpty() &&
               removedInboundNeighbours.isEmpty();
    }

    public Set<UUID> getAddedOutboundNeighbours() {
        return addedOutboundNeighbours;
    }

    public Set<UUID> getRemovedOutboundNeighbours() {
        return removedOutboundNeighbours;
    }

    public boolean isOutboundDistinct() {
        return !addedOutboundNeighbours.stream().anyMatch(removedOutboundNeighbours::contains);
    }


    public Set<UUID> getAddedInboundNeighbours() {
        return addedInboundNeighbours;
    }

    public Set<UUID> getRemovedInboundNeighbours() {
        return removedInboundNeighbours;
    }

    public boolean isInboundDistinct() {
        return !addedInboundNeighbours.stream().anyMatch(removedInboundNeighbours::contains);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AcknowledgeNeighboursMessage that = (AcknowledgeNeighboursMessage) o;

        if (!addedOutboundNeighbours.equals(that.addedOutboundNeighbours)) return false;
        if (!removedOutboundNeighbours.equals(that.removedOutboundNeighbours)) return false;
        if (!addedInboundNeighbours.equals(that.addedInboundNeighbours)) return false;
        return removedInboundNeighbours.equals(that.removedInboundNeighbours);

    }

    @Override
    public int hashCode() {
        int result = addedOutboundNeighbours.hashCode();
        result = 31 * result + removedOutboundNeighbours.hashCode();
        result = 31 * result + addedInboundNeighbours.hashCode();
        result = 31 * result + removedInboundNeighbours.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AcknowledgeNeighboursMessage{" +
               "addedOutboundNeighbours=" + addedOutboundNeighbours +
               ", removedOutboundNeighbours=" + removedOutboundNeighbours +
               ", addedInboundNeighbours=" + addedInboundNeighbours +
               ", removedInboundNeighbours=" + removedInboundNeighbours +
               '}';
    }
}
