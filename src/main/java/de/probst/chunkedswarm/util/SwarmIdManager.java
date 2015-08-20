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
public final class SwarmIdManager {

    // Uuids from this set will not be accepted
    private final Set<UUID> uuidBlacklist = new HashSet<>();

    // Addresses from this set will not be accepted
    private final Set<SocketAddress> addressBlacklist = new HashSet<>();

    // The maps which store the swarm ids
    private final Map<SocketAddress, SwarmId> addressToSwarmId = new HashMap<>();
    private final Map<UUID, SwarmId> uuidToSwarmId = new HashMap<>();

    private synchronized SwarmId createUniqueSwarmId(SocketAddress address) {
        Objects.requireNonNull(address);

        // Check if address is already known
        if (addressBlacklist.contains(address) || addressToSwarmId.containsKey(address)) {
            throw new IllegalArgumentException("addressBlacklist.contains(address) || " +
                                               "addressToSwarmId.containsKey(address)");
        }

        // Find free uuid
        UUID uuid;
        do {
            uuid = newRandomUuid();
        } while (uuidBlacklist.contains(uuid) || uuidToSwarmId.containsKey(uuid));

        // Create a new swarm id
        return new SwarmId(uuid, address);
    }

    public UUID newRandomUuid() {
        return UUID.randomUUID();
    }

    public synchronized boolean blacklistUuid(UUID uuid) {
        return uuidBlacklist.add(uuid);
    }

    public synchronized boolean unblacklistUuid(UUID uuid) {
        return uuidBlacklist.remove(uuid);
    }

    public synchronized boolean blacklistAddress(SocketAddress address) {
        return addressBlacklist.add(address);
    }

    public synchronized boolean unblacklistAddress(SocketAddress address) {
        return addressBlacklist.remove(address);
    }

    public synchronized SwarmId register(SocketAddress address) {
        // Create a new swarm id
        SwarmId swarmId = createUniqueSwarmId(address);

        // Register
        addressToSwarmId.put(address, swarmId);
        uuidToSwarmId.put(swarmId.getUuid(), swarmId);

        return swarmId;
    }

    public synchronized void unregister(SwarmId swarmId) {
        Objects.requireNonNull(swarmId);

        // Check if address is already known
        if (!addressToSwarmId.containsKey(swarmId.getAddress())) {
            throw new IllegalStateException("!addressToSwarmId.containsKey(swarmId.getAddress())");
        }

        // Check if uuid is already known
        if (!uuidToSwarmId.containsKey(swarmId.getUuid())) {
            throw new IllegalStateException("!uuidToSwarmId.containsKey(swarmId.getUuid())");
        }

        // Check combination
        if (!addressToSwarmId.get(swarmId.getAddress()).equals(swarmId) ||
            !uuidToSwarmId.get(swarmId.getUuid()).equals(swarmId)) {
            throw new IllegalStateException("!addressToSwarmId.get(swarmId.getAddress()).equals(swarmId) || " +
                                            "!uuidToSwarmId.get(swarmId.getUuid()).equals(swarmId)");
        }

        // Remove from map
        addressToSwarmId.remove(swarmId.getAddress());
        uuidToSwarmId.remove(swarmId.getUuid());

        // TODO: Maybe cache last 10.000 swarm ids ?
    }
}
