package de.probst.chunkedswarm.net.netty.handler.connection.message;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class AcknowledgeNeighboursMessage implements Serializable {

    private final Set<String> addedOutboundNeighbours;
    private final Set<String> removedOutboundNeighbours;
    private final Set<String> addedInboundNeighbours;
    private final Set<String> removedInboundNeighbours;

    public AcknowledgeNeighboursMessage() {
        this(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>());
    }

    public AcknowledgeNeighboursMessage(Set<String> addedOutboundNeighbours,
                                        Set<String> removedOutboundNeighbours,
                                        Set<String> addedInboundNeighbours,
                                        Set<String> removedInboundNeighbours) {
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

    public Set<String> getAddedOutboundNeighbours() {
        return addedOutboundNeighbours;
    }

    public Set<String> getRemovedOutboundNeighbours() {
        return removedOutboundNeighbours;
    }

    public Set<String> getAddedInboundNeighbours() {
        return addedInboundNeighbours;
    }

    public Set<String> getRemovedInboundNeighbours() {
        return removedInboundNeighbours;
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
