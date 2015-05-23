package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SwarmIdEvent implements Serializable {

    private final Channel channel;

    public static SwarmIdEvent added(Channel channel, SwarmId swarmId) {
        return new SwarmIdEvent(channel, swarmId, Type.Added);
    }

    public static SwarmIdEvent removed(Channel channel, SwarmId swarmId) {
        return new SwarmIdEvent(channel, swarmId, Type.Removed);
    }

    public static SwarmIdEvent acknowledged(Channel channel, SwarmId swarmId) {
        return new SwarmIdEvent(channel, swarmId, Type.Acknowledged);
    }

    public enum Type {
        Added, Removed, Acknowledged
    }

    private final SwarmId swarmId;
    private final Type type;

    public SwarmIdEvent(Channel channel, SwarmId swarmId, Type type) {
        Objects.requireNonNull(channel);
        Objects.requireNonNull(swarmId);
        Objects.requireNonNull(type);
        this.channel = channel;
        this.swarmId = swarmId;
        this.type = type;
    }

    public Channel getChannel() {
        return channel;
    }

    public SwarmId getSwarmId() {
        return swarmId;
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIdEvent that = (SwarmIdEvent) o;

        if (!channel.equals(that.channel)) return false;
        if (!swarmId.equals(that.swarmId)) return false;
        return type == that.type;

    }

    @Override
    public int hashCode() {
        int result = channel.hashCode();
        result = 31 * result + swarmId.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmIdEvent{" +
               "channel=" + channel +
               ", swarmId=" + swarmId +
               ", type=" + type +
               '}';
    }
}
