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

    private final Set<String> addedNeighbours;
    private final Set<String> removedNeighbours;

    public AcknowledgeNeighboursMessage() {
        this(new HashSet<>(), new HashSet<>());
    }

    public AcknowledgeNeighboursMessage(Set<String> addedNeighbours, Set<String> removedNeighbours) {
        Objects.requireNonNull(addedNeighbours);
        Objects.requireNonNull(removedNeighbours);
        this.addedNeighbours = addedNeighbours;
        this.removedNeighbours = removedNeighbours;
    }

    public boolean isEmpty() {
        return addedNeighbours.isEmpty() && removedNeighbours.isEmpty();
    }

    public Set<String> getAddedNeighbours() {
        return addedNeighbours;
    }

    public Set<String> getRemovedNeighbours() {
        return removedNeighbours;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AcknowledgeNeighboursMessage that = (AcknowledgeNeighboursMessage) o;

        if (!addedNeighbours.equals(that.addedNeighbours)) return false;
        return removedNeighbours.equals(that.removedNeighbours);

    }

    @Override
    public int hashCode() {
        int result = addedNeighbours.hashCode();
        result = 31 * result + removedNeighbours.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AcknowledgeNeighboursMessage{" +
               "addedNeighbours=" + addedNeighbours +
               ", removedNeighbours=" + removedNeighbours +
               '}';
    }
}
