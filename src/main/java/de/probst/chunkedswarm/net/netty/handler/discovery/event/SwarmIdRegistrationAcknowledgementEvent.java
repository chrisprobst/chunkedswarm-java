package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SwarmIdRegistrationAcknowledgementEvent implements Serializable {

    private final Channel channel;
    private final SwarmId swarmId;

    public SwarmIdRegistrationAcknowledgementEvent(Channel channel, SwarmId swarmId) {
        Objects.requireNonNull(channel);
        Objects.requireNonNull(swarmId);
        this.channel = channel;
        this.swarmId = swarmId;
    }

    public Channel getChannel() {
        return channel;
    }

    public SwarmId getSwarmId() {
        return swarmId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIdRegistrationAcknowledgementEvent that = (SwarmIdRegistrationAcknowledgementEvent) o;

        if (!channel.equals(that.channel)) return false;
        return swarmId.equals(that.swarmId);

    }

    @Override
    public int hashCode() {
        int result = channel.hashCode();
        result = 31 * result + swarmId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmIdRegistrationAcknowledgementEvent{" +
               "channel=" + channel +
               ", swarmId=" + swarmId +
               '}';
    }
}
