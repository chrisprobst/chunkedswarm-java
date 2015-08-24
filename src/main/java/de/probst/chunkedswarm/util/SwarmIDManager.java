package de.probst.chunkedswarm.util;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIDManager {

    // UUIDs from this set will not be accepted
    private final Set<UUID> uuidBlacklist = new HashSet<>();

    // Addresses from this set will not be accepted
    private final Set<SocketAddress> addressBlacklist = new HashSet<>();

    // The maps which store the swarm ids
    private final Map<SocketAddress, SwarmID> addressToSwarmID = new HashMap<>();
    private final Map<UUID, SwarmID> uuidToSwarmID = new HashMap<>();

    private synchronized SwarmID createUniqueSwarmID(SocketAddress address) {
        Objects.requireNonNull(address);

        // Check if address is already known
        if (addressBlacklist.contains(address) || addressToSwarmID.containsKey(address)) {
            throw new IllegalArgumentException("addressBlacklist.contains(address) || " +
                                               "addressToSwarmID.containsKey(address)");
        }

        // Find free uuid
        UUID uuid;
        do {
            uuid = newRandomUUID();
        } while (uuidBlacklist.contains(uuid) || uuidToSwarmID.containsKey(uuid));

        // Create a new swarm id
        return new SwarmID(uuid, address);
    }

    public UUID newRandomUUID() {
        return UUID.randomUUID();
    }

    public synchronized boolean blacklistUUID(UUID uuid) {
        return uuidBlacklist.add(uuid);
    }

    public synchronized boolean unblacklistUUID(UUID uuid) {
        return uuidBlacklist.remove(uuid);
    }

    public synchronized boolean blacklistAddress(SocketAddress address) {
        return addressBlacklist.add(address);
    }

    public synchronized boolean unblacklistAddress(SocketAddress address) {
        return addressBlacklist.remove(address);
    }

    public synchronized SwarmID register(SocketAddress address) {
        // Create a new swarm id
        SwarmID swarmID = createUniqueSwarmID(address);

        // Register
        addressToSwarmID.put(address, swarmID);
        uuidToSwarmID.put(swarmID.getUUID(), swarmID);

        return swarmID;
    }

    public synchronized void unregister(SwarmID swarmID) {
        Objects.requireNonNull(swarmID);

        // Check if address is already known
        if (!addressToSwarmID.containsKey(swarmID.getAddress())) {
            throw new IllegalStateException("!addressToSwarmID.containsKey(swarmID.getAddress())");
        }

        // Check if uuid is already known
        if (!uuidToSwarmID.containsKey(swarmID.getUUID())) {
            throw new IllegalStateException("!uuidToSwarmID.containsKey(swarmID.getUUID())");
        }

        // Check combination
        if (!addressToSwarmID.get(swarmID.getAddress()).equals(swarmID) ||
            !uuidToSwarmID.get(swarmID.getUUID()).equals(swarmID)) {
            throw new IllegalStateException("!addressToSwarmID.get(swarmID.getAddress()).equals(swarmID) || " +
                                            "!uuidToSwarmID.get(swarmID.getUUID()).equals(swarmID)");
        }

        // Remove from map
        addressToSwarmID.remove(swarmID.getAddress());
        uuidToSwarmID.remove(swarmID.getUUID());

        // TODO: Maybe cache last 10.000 swarm ids ?
    }
}
