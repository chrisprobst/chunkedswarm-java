package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SwarmIdRegistrationEvent implements Serializable {

    private final Channel channel;

    public enum Type {
        Registered, Unregistered, Acknowledged
    }

    private final SwarmId swarmId;
    private final Type type;

    public SwarmIdRegistrationEvent(Channel channel, SwarmId swarmId, Type type) {
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

        SwarmIdRegistrationEvent that = (SwarmIdRegistrationEvent) o;

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
        return "SwarmIdRegistrationEvent{" +
               "channel=" + channel +
               ", swarmId=" + swarmId +
               ", type=" + type +
               '}';
    }
}
