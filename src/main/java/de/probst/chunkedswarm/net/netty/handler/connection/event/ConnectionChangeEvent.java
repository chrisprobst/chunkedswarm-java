package de.probst.chunkedswarm.net.netty.handler.connection.event;

import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class ConnectionChangeEvent {

    public enum Direction {
        Inbound, Outbound
    }

    public enum Type {
        ConnectionRefused, Connected, Disconnected
    }

    private final SwarmID swarmID;
    private final Channel channel;
    private final Direction direction;
    private final Type type;

    public ConnectionChangeEvent(SwarmID swarmID,
                                 Channel channel,
                                 Direction direction,
                                 Type type) {
        Objects.requireNonNull(swarmID);
        Objects.requireNonNull(channel);
        Objects.requireNonNull(direction);
        Objects.requireNonNull(type);
        this.swarmID = swarmID;
        this.channel = channel;
        this.direction = direction;
        this.type = type;
    }

    public SwarmID getSwarmID() {
        return swarmID;
    }

    public Channel getChannel() {
        return channel;
    }

    public Direction getDirection() {
        return direction;
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionChangeEvent that = (ConnectionChangeEvent) o;

        if (!swarmID.equals(that.swarmID)) return false;
        if (!channel.equals(that.channel)) return false;
        if (direction != that.direction) return false;
        return type == that.type;

    }

    @Override
    public int hashCode() {
        int result = swarmID.hashCode();
        result = 31 * result + channel.hashCode();
        result = 31 * result + direction.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ForwarderConnectionEvent{" +
               "swarmID=" + swarmID +
               ", channel=" + channel +
               ", direction=" + direction +
               ", type=" + type +
               '}';
    }
}
