package de.probst.chunkedswarm.net.netty.handler.discovery.event;

import de.probst.chunkedswarm.util.SwarmIdSet;

import java.util.Collections;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class SwarmIdCollectionEvent {

    private final SwarmIdSet swarmIdSet;

    public SwarmIdCollectionEvent(SwarmIdSet swarmIdSet) {
        this.swarmIdSet = new SwarmIdSet(Collections.unmodifiableSet(swarmIdSet.get()));
    }

    public SwarmIdSet getSwarmIdSet() {
        return swarmIdSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmIdCollectionEvent that = (SwarmIdCollectionEvent) o;

        return swarmIdSet.equals(that.swarmIdSet);
    }

    @Override
    public int hashCode() {
        return swarmIdSet.hashCode();
    }

    @Override
    public String toString() {
        return "SwarmIdCollectionEvent{" +
               "swarmIdSet=" + swarmIdSet +
               '}';
    }
}
