package de.probst.chunkedswarm.net.netty.handler.connection.event;

import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class NeighbourConnectionEvent {

    public enum Type {
        Connected, Disconnected
    }

    private final SwarmId swarmId;
    private final Channel channel;
    private final Type type;

    public NeighbourConnectionEvent(SwarmId swarmId,
                                    Channel channel,
                                    Type type) {
        Objects.requireNonNull(swarmId);
        Objects.requireNonNull(channel);
        Objects.requireNonNull(type);
        this.swarmId = swarmId;
        this.channel = channel;
        this.type = type;
    }

    public SwarmId getSwarmId() {
        return swarmId;
    }

    public Channel getChannel() {
        return channel;
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NeighbourConnectionEvent that = (NeighbourConnectionEvent) o;

        if (!swarmId.equals(that.swarmId)) return false;
        if (!channel.equals(that.channel)) return false;
        return type == that.type;

    }

    @Override
    public int hashCode() {
        int result = swarmId.hashCode();
        result = 31 * result + channel.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionEvent{" +
               "swarmId=" + swarmId +
               ", channel=" + channel +
               ", type=" + type +
               '}';
    }
}
