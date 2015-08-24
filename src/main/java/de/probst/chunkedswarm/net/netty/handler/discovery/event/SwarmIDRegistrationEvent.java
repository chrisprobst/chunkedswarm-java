package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SwarmIDRegistrationEvent implements Serializable {

    public enum Type {
        Registered, Unregistered
    }

    private final Channel channel;
    private final SwarmID swarmID;
    private final Type type;

    public SwarmIDRegistrationEvent(Channel channel, SwarmID swarmID, Type type) {
        Objects.requireNonNull(channel);
        Objects.requireNonNull(swarmID);
        Objects.requireNonNull(type);
        this.channel = channel;
        this.swarmID = swarmID;
        this.type = type;
    }

    public Channel getChannel() {
        return channel;
    }

    public SwarmID getSwarmID() {
        return swarmID;
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIDRegistrationEvent that = (SwarmIDRegistrationEvent) o;

        if (!channel.equals(that.channel)) return false;
        if (!swarmID.equals(that.swarmID)) return false;
        return type == that.type;

    }

    @Override
    public int hashCode() {
        int result = channel.hashCode();
        result = 31 * result + swarmID.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmIDRegistrationEvent{" +
               "channel=" + channel +
               ", swarmID=" + swarmID +
               ", type=" + type +
               '}';
    }
}
