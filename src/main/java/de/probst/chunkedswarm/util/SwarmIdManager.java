package de.probst.chunkedswarm.util;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdManager {

    // The maps which store the swarm ids
    private final Map<SocketAddress, SwarmId> addressToSwarmId = new HashMap<>();
    private final Map<String, SwarmId> uuidToSwarmId = new HashMap<>();

    public synchronized SwarmId register(SocketAddress address) {
        Objects.requireNonNull(address);

        // Check if address is already known
        if (addressToSwarmId.containsKey(address)) {
            throw new IllegalStateException("addressToSwarmId.containsKey(socketAddress)");
        }

        // Find free uuid
        String uuid;
        do {
            uuid = UUID.randomUUID().toString();
        } while (uuidToSwarmId.containsKey(uuid));

        // Create a new swarm id
        SwarmId swarmId = new SwarmId(uuid, address);

        // Register
        addressToSwarmId.put(address, swarmId);
        uuidToSwarmId.put(uuid, swarmId);

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
    }
}
