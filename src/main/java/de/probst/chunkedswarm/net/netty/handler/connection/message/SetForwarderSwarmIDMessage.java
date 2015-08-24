package de.probst.chunkedswarm.net.netty.handler.connection.message;

import de.probst.chunkedswarm.util.SwarmID;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SetForwarderSwarmIDMessage implements Serializable {

    private final SwarmID forwarderSwarmID;

    public SetForwarderSwarmIDMessage(SwarmID forwarderSwarmID) {
        Objects.requireNonNull(forwarderSwarmID);
        this.forwarderSwarmID = forwarderSwarmID;
    }

    public SwarmID getForwarderSwarmID() {
        return forwarderSwarmID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SetForwarderSwarmIDMessage that = (SetForwarderSwarmIDMessage) o;

        return forwarderSwarmID.equals(that.forwarderSwarmID);

    }

    @Override
    public int hashCode() {
        return forwarderSwarmID.hashCode();
    }

    @Override
    public String toString() {
        return "SetForwarderSwarmIDMessage{" +
               "forwarderSwarmID=" + forwarderSwarmID +
               '}';
    }
}
