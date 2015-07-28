package de.probst.chunkedswarm.net.netty.handler.connection.message;

import de.probst.chunkedswarm.util.SwarmId;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SetForwarderSwarmIdMessage implements Serializable {

    private final SwarmId forwarderSwarmId;

    public SetForwarderSwarmIdMessage(SwarmId forwarderSwarmId) {
        Objects.requireNonNull(forwarderSwarmId);
        this.forwarderSwarmId = forwarderSwarmId;
    }

    public SwarmId getForwarderSwarmId() {
        return forwarderSwarmId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetForwarderSwarmIdMessage that = (SetForwarderSwarmIdMessage) o;

        return forwarderSwarmId.equals(that.forwarderSwarmId);

    }

    @Override
    public int hashCode() {
        return forwarderSwarmId.hashCode();
    }

    @Override
    public String toString() {
        return "SetForwarderSwarmIdMessage{" +
               "forwarderSwarmId=" + forwarderSwarmId +
               '}';
    }
}
