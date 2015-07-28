package de.probst.chunkedswarm.net.netty.handler.connection.event;

import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
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

    // Store all acknowledged neighbours here
    private final Set<String> acknowledgedNeighbours;

    // The message, which changed the acknowledged neighbours
    private final Optional<AcknowledgeNeighboursMessage> acknowledgeNeighboursMessage;

    // The channel handler context
    private final ChannelHandlerContext ctx;

    // The local swarm id
    private final SwarmId localSwarmId;

    public AcknowledgedNeighboursEvent(Type type,
                                       Set<String> acknowledgedNeighbours,
                                       Optional<AcknowledgeNeighboursMessage> acknowledgeNeighboursMessage,
                                       ChannelHandlerContext ctx,
                                       SwarmId localSwarmId) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(acknowledgedNeighbours);
        Objects.requireNonNull(acknowledgeNeighboursMessage);
        Objects.requireNonNull(ctx);
        Objects.requireNonNull(localSwarmId);

        this.type = type;
        this.acknowledgedNeighbours = new HashSet<>(acknowledgedNeighbours);
        this.acknowledgeNeighboursMessage = acknowledgeNeighboursMessage;
        this.ctx = ctx;
        this.localSwarmId = localSwarmId;
    }

    public Type getType() {
        return type;
    }

    public Set<String> getAcknowledgedNeighbours() {
        return acknowledgedNeighbours;
    }

    public Optional<AcknowledgeNeighboursMessage> getAcknowledgeNeighboursMessage() {
        return acknowledgeNeighboursMessage;
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
        if (!acknowledgedNeighbours.equals(that.acknowledgedNeighbours)) return false;
        if (!acknowledgeNeighboursMessage.equals(that.acknowledgeNeighboursMessage)) return false;
        if (!ctx.equals(that.ctx)) return false;
        return localSwarmId.equals(that.localSwarmId);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + acknowledgedNeighbours.hashCode();
        result = 31 * result + acknowledgeNeighboursMessage.hashCode();
        result = 31 * result + ctx.hashCode();
        result = 31 * result + localSwarmId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AcknowledgedNeighboursEvent{" +
               "type=" + type +
               ", acknowledgedNeighbours=" + acknowledgedNeighbours +
               ", acknowledgeNeighboursMessage=" + acknowledgeNeighboursMessage +
               ", ctx=" + ctx +
               ", localSwarmId=" + localSwarmId +
               '}';
    }
}
