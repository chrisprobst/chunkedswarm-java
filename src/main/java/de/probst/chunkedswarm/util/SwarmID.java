package de.probst.chunkedswarm.util;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.Objects;

public final class SwarmId implements Comparable<SwarmId>, Serializable {

    private final String uuid;
    private final SocketAddress address;

    public SwarmId(String uuid, SocketAddress address) {
        Objects.requireNonNull(uuid);
        Objects.requireNonNull(address);
        this.uuid = uuid;
        this.address = address;
    }

    public String getUuid() {
        return uuid;
    }

    public SocketAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmId swarmID = (SwarmId) o;

        if (!uuid.equals(swarmID.uuid)) return false;
        return address.equals(swarmID.address);

    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + address.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SwarmID{" +
               "uuid='" + uuid + '\'' +
               ", address=" + address +
               '}';
    }

    @Override
    public int compareTo(SwarmId o) {
        return uuid.compareTo(o.uuid);
    }
}