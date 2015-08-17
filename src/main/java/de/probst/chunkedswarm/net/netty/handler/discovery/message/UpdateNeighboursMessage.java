package de.probst.chunkedswarm.net.netty.handler.discovery.message;

import de.probst.chunkedswarm.util.SwarmId;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class UpdateNeighboursMessage implements Serializable {

    private final Set<SwarmId> addNeighbours;
    private final Set<SwarmId> removeNeighbours;

    public UpdateNeighboursMessage() {
        this(new HashSet<>(), new HashSet<>());
    }

    public UpdateNeighboursMessage(Set<SwarmId> addNeighbours, Set<SwarmId> removeNeighbours) {
        Objects.requireNonNull(addNeighbours);
        Objects.requireNonNull(removeNeighbours);
        this.addNeighbours = addNeighbours;
        this.removeNeighbours = removeNeighbours;
    }

    public boolean isEmpty() {
        return addNeighbours.isEmpty() && removeNeighbours.isEmpty();
    }

    public Set<SwarmId> getAddNeighbours() {
        return addNeighbours;
    }

    public Set<SwarmId> getRemoveNeighbours() {
        return removeNeighbours;
    }

    public boolean isDistinct() {
        return !addNeighbours.stream().anyMatch(removeNeighbours::contains);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateNeighboursMessage that = (UpdateNeighboursMessage) o;

        if (!addNeighbours.equals(that.addNeighbours)) return false;
        return removeNeighbours.equals(that.removeNeighbours);

    }

    @Override
    public int hashCode() {
        int result = addNeighbours.hashCode();
        result = 31 * result + removeNeighbours.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "UpdateNeighboursMessage{" +
               "addNeighbours=" + addNeighbours +
               ", removeNeighbours=" + removeNeighbours +
               '}';
    }
}
