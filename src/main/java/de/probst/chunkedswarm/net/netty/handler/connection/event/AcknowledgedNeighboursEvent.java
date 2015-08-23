package de.probst.chunkedswarm.net.netty.handler.connection.event;

import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 02.06.15
 */
public final class AcknowledgedNeighboursEvent {

    public enum Type {
        Register, Update, Unregister
    }

    // The type of this event
    private final Type type;

    // Store all acknowledged outbound neighbours here
    private final Set<UUID> acknowledgedOutboundNeighbours;

    // Store all acknowledged inbound neighbours here
    private final Set<UUID> acknowledgedInboundNeighbours;

    // The message, which changed the acknowledged neighbours
    private final Optional<AcknowledgeNeighboursMessage> acknowledgeNeighboursMessage;

    // The local swarm id
    private final SwarmID localSwarmID;

    // The channel
    private final Channel channel;

    public AcknowledgedNeighboursEvent(Type type,
                                       Set<UUID> acknowledgedOutboundNeighbours,
                                       Set<UUID> acknowledgedInboundNeighbours,
                                       Optional<AcknowledgeNeighboursMessage> acknowledgeNeighboursMessage,
                                       SwarmID localSwarmID,
                                       Channel channel) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(acknowledgedOutboundNeighbours);
        Objects.requireNonNull(acknowledgedInboundNeighbours);
        Objects.requireNonNull(acknowledgeNeighboursMessage);
        Objects.requireNonNull(localSwarmID);
        Objects.requireNonNull(channel);

        this.type = type;
        this.acknowledgedOutboundNeighbours = Collections.unmodifiableSet(new HashSet<>(acknowledgedOutboundNeighbours));
        this.acknowledgedInboundNeighbours = Collections.unmodifiableSet(new HashSet<>(acknowledgedInboundNeighbours));
        this.acknowledgeNeighboursMessage = acknowledgeNeighboursMessage;
        this.localSwarmID = localSwarmID;
        this.channel = channel;
    }

    public Type getType() {
        return type;
    }

    public Set<UUID> getAcknowledgedOutboundNeighbours() {
        return acknowledgedOutboundNeighbours;
    }

    public Set<UUID> getAcknowledgedInboundNeighbours() {
        return acknowledgedInboundNeighbours;
    }

    public Optional<AcknowledgeNeighboursMessage> getAcknowledgeNeighboursMessage() {
        return acknowledgeNeighboursMessage;
    }

    public SwarmID getLocalSwarmID() {
        return localSwarmID;
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AcknowledgedNeighboursEvent that = (AcknowledgedNeighboursEvent) o;

        if (type != that.type) return false;
        if (!acknowledgedOutboundNeighbours.equals(that.acknowledgedOutboundNeighbours)) return false;
        if (!acknowledgedInboundNeighbours.equals(that.acknowledgedInboundNeighbours)) return false;
        if (!acknowledgeNeighboursMessage.equals(that.acknowledgeNeighboursMessage)) return false;
        if (!localSwarmID.equals(that.localSwarmID)) return false;
        return channel.equals(that.channel);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + acknowledgedOutboundNeighbours.hashCode();
        result = 31 * result + acknowledgedInboundNeighbours.hashCode();
        result = 31 * result + acknowledgeNeighboursMessage.hashCode();
        result = 31 * result + localSwarmID.hashCode();
        result = 31 * result + channel.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AcknowledgedNeighboursEvent{" +
               "type=" + type +
               ", acknowledgedOutboundNeighbours=" + acknowledgedOutboundNeighbours +
               ", acknowledgedInboundNeighbours=" + acknowledgedInboundNeighbours +
               ", acknowledgeNeighboursMessage=" + acknowledgeNeighboursMessage +
               ", localSwarmID=" + localSwarmID +
               ", channel=" + channel +
               '}';
    }
}
