package de.probst.chunkedswarm.net.netty.handler.connection.event;

import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 02.06.15
 */
public final class AcknowledgedNeighboursEvent {

    public enum Type {
        Update, Dispose
    }

    // The type of this event
    private final Type type;

    // Store all known neighbours here
    private final Map<String, SwarmId> knownNeighbours;

    // Store all acknowledged neighbours here
    private final Set<String> acknowledgedNeighbours;

    // The channel handler context
    private final ChannelHandlerContext ctx;

    // The local swarm id
    private final SwarmId localSwarmId;

    public AcknowledgedNeighboursEvent(Type type,
                                       Map<String, SwarmId> knownNeighbours,
                                       Set<String> acknowledgedNeighbours,
                                       ChannelHandlerContext ctx,
                                       SwarmId localSwarmId) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(knownNeighbours);
        Objects.requireNonNull(acknowledgedNeighbours);
        Objects.requireNonNull(ctx);
        Objects.requireNonNull(localSwarmId);

        this.type = type;
        this.knownNeighbours = new HashMap<>(knownNeighbours);
        this.acknowledgedNeighbours = new HashSet<>(acknowledgedNeighbours);
        this.ctx = ctx;
        this.localSwarmId = localSwarmId;
    }

    public Type getType() {
        return type;
    }

    public Map<String, SwarmId> getKnownNeighbours() {
        return knownNeighbours;
    }

    public Set<String> getAcknowledgedNeighbours() {
        return acknowledgedNeighbours;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public SwarmId getLocalSwarmId() {
        return localSwarmId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AcknowledgedNeighboursEvent that = (AcknowledgedNeighboursEvent) o;

        if (type != that.type) return false;
        if (!knownNeighbours.equals(that.knownNeighbours)) return false;
        if (!acknowledgedNeighbours.equals(that.acknowledgedNeighbours)) return false;
        if (!ctx.equals(that.ctx)) return false;
        return localSwarmId.equals(that.localSwarmId);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + knownNeighbours.hashCode();
        result = 31 * result + acknowledgedNeighbours.hashCode();
        result = 31 * result + ctx.hashCode();
        result = 31 * result + localSwarmId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AcknowledgedNeighboursEvent{" +
               "type=" + type +
               ", knownNeighbours=" + knownNeighbours +
               ", acknowledgedNeighbours=" + acknowledgedNeighbours +
               ", ctx=" + ctx +
               ", localSwarmId=" + localSwarmId +
               '}';
    }
}
