package de.probst.chunkedswarm.net.netty.handler.connection.event;

import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.08.15
 */
public final class ConnectionEvent {

    private final ConnectionChangeEvent connectionChangeEvent;
    private final Map<SwarmID, Channel> engagedConnections;

    public ConnectionEvent(ConnectionChangeEvent connectionChangeEvent, Map<SwarmID, Channel> engagedConnections) {
        Objects.requireNonNull(connectionChangeEvent);
        Objects.requireNonNull(engagedConnections);
        this.connectionChangeEvent = connectionChangeEvent;
        this.engagedConnections = Collections.unmodifiableMap(new HashMap<>(engagedConnections));
    }

    public ConnectionChangeEvent getConnectionChangeEvent() {
        return connectionChangeEvent;
    }

    public Map<SwarmID, Channel> getEngagedConnections() {
        return engagedConnections;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionEvent that = (ConnectionEvent) o;

        if (!connectionChangeEvent.equals(that.connectionChangeEvent)) return false;
        return engagedConnections.equals(that.engagedConnections);

    }

    @Override
    public int hashCode() {
        int result = connectionChangeEvent.hashCode();
        result = 31 * result + engagedConnections.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionEvent{" +
               "connectionChangeEvent=" + connectionChangeEvent +
               ", engagedConnections=" + engagedConnections +
               '}';
    }
}
