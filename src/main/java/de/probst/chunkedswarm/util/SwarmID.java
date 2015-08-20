package de.probst.chunkedswarm.util;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.UUID;

public final class SwarmID implements Serializable {

    private final UUID uuid;
    private final SocketAddress address;

    public SwarmID(UUID uuid, SocketAddress address) {
        Objects.requireNonNull(uuid);
        Objects.requireNonNull(address);
        this.uuid = uuid;
        this.address = address;
    }

    public UUID getUUID() {
        return uuid;
    }

    public SocketAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SwarmID swarmID = (SwarmID) o;

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
               "uuid=" + uuid +
               ", address=" + address +
               '}';
    }
}