package de.probst.chunkedswarm.net.netty.handler.discovery.message;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 23.05.15
 */
public final class SetCollectorAddressMessage implements Serializable {

    private final SocketAddress collectorAddress;

    public SetCollectorAddressMessage(SocketAddress collectorAddress) {
        Objects.requireNonNull(collectorAddress);
        this.collectorAddress = collectorAddress;
    }

    public SocketAddress getCollectorAddress() {
        return collectorAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SetCollectorAddressMessage that = (SetCollectorAddressMessage) o;

        return collectorAddress.equals(that.collectorAddress);

    }

    @Override
    public int hashCode() {
        return collectorAddress.hashCode();
    }

    @Override
    public String toString() {
        return "SetCollectorAddressMessage{" +
               "collectorAddress=" + collectorAddress +
               '}';
    }
}
