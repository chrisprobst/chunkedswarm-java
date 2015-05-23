package de.probst.chunkedswarm.net.netty.handler.discovery.message;

import de.probst.chunkedswarm.util.SwarmId;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SetLocalSwarmIdMessage implements Serializable {

    private final SwarmId localSwarmId;

    public SetLocalSwarmIdMessage(SwarmId localSwarmId) {
        Objects.requireNonNull(localSwarmId);
        this.localSwarmId = localSwarmId;
    }

    public SwarmId getLocalSwarmId() {
        return localSwarmId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetLocalSwarmIdMessage that = (SetLocalSwarmIdMessage) o;

        return localSwarmId.equals(that.localSwarmId);

    }

    @Override
    public int hashCode() {
        return localSwarmId.hashCode();
    }

    @Override
    public String toString() {
        return "SetLocalSwarmIdMessage{" +
               "localSwarmId=" + localSwarmId +
               '}';
    }
}
