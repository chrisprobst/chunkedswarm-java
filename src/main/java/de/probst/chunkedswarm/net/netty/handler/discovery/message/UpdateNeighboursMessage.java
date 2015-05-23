package de.probst.chunkedswarm.net.netty.handler.discovery.message;

import de.probst.chunkedswarm.util.SwarmIdSet;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class UpdateNeighboursMessage implements Serializable {

    private final SwarmIdSet addNeighbours;
    private final SwarmIdSet removeNeighbours;

    public UpdateNeighboursMessage() {
        this(new SwarmIdSet(), new SwarmIdSet());
    }

    public UpdateNeighboursMessage(SwarmIdSet addNeighbours, SwarmIdSet removeNeighbours) {
        Objects.requireNonNull(addNeighbours);
        Objects.requireNonNull(removeNeighbours);
        this.addNeighbours = addNeighbours;
        this.removeNeighbours = removeNeighbours;
    }

    public boolean isEmpty() {
        return addNeighbours.get().isEmpty() && removeNeighbours.get().isEmpty();
    }

    public SwarmIdSet getAddNeighbours() {
        return addNeighbours;
    }

    public SwarmIdSet getRemoveNeighbours() {
        return removeNeighbours;
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
