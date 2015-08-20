package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SwarmIDRegistrationAcknowledgementEvent implements Serializable {

    private final Channel channel;
    private final SwarmID swarmID;

    public SwarmIDRegistrationAcknowledgementEvent(Channel channel, SwarmID swarmID) {
        Objects.requireNonNull(channel);
        Objects.requireNonNull(swarmID);
        this.channel = channel;
        this.swarmID = swarmID;
    }

    public Channel getChannel() {
        return channel;
    }

    public SwarmID getSwarmID() {
        return swarmID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIDRegistrationAcknowledgementEvent that = (SwarmIDRegistrationAcknowledgementEvent) o;

        if (!channel.equals(that.channel)) return false;
        return swarmID.equals(that.swarmID);

    }

    @Override
    public int hashCode() {
        int result = channel.hashCode();
        result = 31 * result + swarmID.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmIDRegistrationAcknowledgementEvent{" +
               "channel=" + channel +
               ", swarmID=" + swarmID +
               '}';
    }
}
