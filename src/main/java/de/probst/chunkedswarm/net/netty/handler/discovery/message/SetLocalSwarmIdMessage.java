package de.probst.chunkedswarm.net.netty.handler.discovery.message;

import de.probst.chunkedswarm.util.SwarmID;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SetLocalSwarmIDMessage implements Serializable {

    private final SwarmID localSwarmID;

    public SetLocalSwarmIDMessage(SwarmID localSwarmID) {
        Objects.requireNonNull(localSwarmID);
        this.localSwarmID = localSwarmID;
    }

    public SwarmID getLocalSwarmID() {
        return localSwarmID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetLocalSwarmIDMessage that = (SetLocalSwarmIDMessage) o;

        return localSwarmID.equals(that.localSwarmID);

    }

    @Override
    public int hashCode() {
        return localSwarmID.hashCode();
    }

    @Override
    public String toString() {
        return "SetLocalSwarmIDMessage{" +
               "localSwarmID=" + localSwarmID +
               '}';
    }
}
