package de.probst.chunkedswarm.util;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
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
}
