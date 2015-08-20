package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushEvent;
import de.probst.chunkedswarm.util.BlockHeader;
import de.probst.chunkedswarm.util.Graph;
import de.probst.chunkedswarm.util.Hash;
import de.probst.chunkedswarm.util.Hasher;
import de.probst.chunkedswarm.util.NodeGroup;
import de.probst.chunkedswarm.util.NodeGroups;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Handler sends to owner channel:
 * - PushEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 18.08.15
 */
public final class PushHandler extends ChannelHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PushHandler.class);

    // All channels
    private final ChannelGroup allChannels;

    // The master uuid, so nobody can choose this uuid
    private final UUID masterUUID;

    // Used to store all incoming events
    private final Map<UUID, AcknowledgedNeighboursEvent> acknowledgedNeighbours = new HashMap<>();

    // Used to track pending pushes
    private final Set<ChannelGroupFuture> pendingPushes = new LinkedHashSet<>();

    // Used to compute hashes
    private final Hasher hasher = new Hasher();

    private BlockHeader createBlockHeader(PushEvent evt, int chunkCount) {

        // Create a split version of the block
        ByteBuffer dup = evt.getPayload().duplicate();
        int size = dup.remaining();

        // Compute hash for payload
        Hash hash = hasher.computeHash(dup.duplicate());

        // Compute all chunk hashes
        List<Hash> chunkHashes = IntStream.range(0, chunkCount).mapToObj(i -> {
            // Compute limit for chunk and return computed chunk
            dup.limit(dup.position() + BlockHeader.computeChunkSize(size, chunkCount, i));
            return hasher.computeHash(dup);
        }).collect(Collectors.toList());

        // Create the block header for the push event
        return new BlockHeader(hash,
                               chunkHashes,
                               evt.getSequence(),
                               evt.getPriority(),
                               size,
                               evt.getDuration());
    }

    private NodeGroups<UUID> computeMeshes() {
        // Create graphs to compute the meshes
        Graph<UUID> outboundGraph = new Graph<>();
        Graph<UUID> inboundGraph = new Graph<>();

        // The master node groups
        NodeGroup<UUID> masterOutboundNodeGroup = new NodeGroup<>();
        NodeGroup<UUID> masterInboundNodeGroup = new NodeGroup<>();

        // Create node groups for each node
        acknowledgedNeighbours.values().forEach(evt -> {
            NodeGroup<UUID> outboundNodeGroup = new NodeGroup<>();
            NodeGroup<UUID> inboundNodeGroup = new NodeGroup<>();

            // Add all acknowledged neighbours into group
            outboundNodeGroup.getNodes().addAll(evt.getAcknowledgedOutboundNeighbours());
            inboundNodeGroup.getNodes().addAll(evt.getAcknowledgedInboundNeighbours());

            // Of course, each node is connected with the master
            outboundNodeGroup.getNodes().add(masterUUID);
            inboundNodeGroup.getNodes().add(masterUUID);

            // The master is connected with this node
            masterOutboundNodeGroup.getNodes().add(evt.getLocalSwarmID().getUUID());
            masterInboundNodeGroup.getNodes().add(evt.getLocalSwarmID().getUUID());

            // Put node groups into graphs
            outboundGraph.getNodes().put(evt.getLocalSwarmID().getUUID(), outboundNodeGroup);
            inboundGraph.getNodes().put(evt.getLocalSwarmID().getUUID(), inboundNodeGroup);
        });

        // Put the view of the master into the graphs
        outboundGraph.getNodes().put(masterUUID, masterOutboundNodeGroup);
        inboundGraph.getNodes().put(masterUUID, masterInboundNodeGroup);

        logger.info("Inbound graph size: " +
                    inboundGraph.getNodes().size() +
                    ", Outbound graph size: " +
                    outboundGraph.getNodes().size());

        // Compute the meshes
        return outboundGraph.findMeshes(masterUUID, inboundGraph);
    }

    private Map<Channel, Integer> nodeGroupToChunkMap(NodeGroup<UUID> nodeGroup) {
        PrimitiveIterator.OfInt idxs = IntStream.range(0, nodeGroup.getNodes().size()).iterator();
        return nodeGroup.getNodes()
                        .stream()
                        .map(acknowledgedNeighbours::get)
                        .map(AcknowledgedNeighboursEvent::getChannel)
                        .collect(Collectors.toMap(c -> c, c -> idxs.next()));
    }

    private void handleAcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent evt) {
        switch (evt.getType()) {
            case Update:
                acknowledgedNeighbours.put(evt.getLocalSwarmID().getUUID(), evt);
                break;
            case Dispose:
                acknowledgedNeighbours.remove(evt.getLocalSwarmID().getUUID());
                break;
        }
    }

    private void handlePushEvent(PushEvent evt) {

        // At first, compute all meshes
        // Default policy takes just the biggest mesh
        NodeGroups<UUID> meshes = computeMeshes();
        if (meshes.getGroups().isEmpty()) {
            logger.info("Nothing to push, node group empty");
            return;
        }

        // Choose the largest group by default
        NodeGroup<UUID> largestGroup = meshes.getGroups().get(0);

        // Create chunk map from largest node group
        Map<Channel, Integer> chunkMap = nodeGroupToChunkMap(largestGroup);

        // Collect vars
        ByteBuffer payload = evt.getPayload().duplicate();
        int chunkCount = chunkMap.size();

        // TODO: Maybe just broadcasting ?
        // Too small for chunking
        if (payload.remaining() < chunkCount) {
            logger.warn("payload.remaining() < chunkCount");
            return;
        }

        // Create the block header
        BlockHeader blockHeader = createBlockHeader(evt, chunkCount);

        // Send block to all peers
        ChannelGroupFuture pushFuture = allChannels.writeAndFlush(new PushWriteRequest(blockHeader, payload, chunkMap),
                                                                  chunkMap.keySet()::contains);

        // Compute statistics
        long count = StreamSupport.stream(pushFuture.spliterator(), false).count();
        logger.info("Pushing: " + blockHeader);

        // Keep track of future
        pendingPushes.add(pushFuture);
        pushFuture.addListener(fut -> {

            // Remove from pending
            pendingPushes.remove(pushFuture);

            // Compute statistics
            long failed = !pushFuture.isSuccess() ? StreamSupport.stream(pushFuture.cause().spliterator(), false)
                                                                 .count() : 0;
            String rate = (count - failed) + "/" + count;
            logger.info("Pushed: " + blockHeader + ", Success: " + rate);

            if (!pushFuture.isSuccess()) {
                pushFuture.cause().iterator().forEachRemaining(e -> logger.warn("Partial push failure", e.getValue()));
            }
        });
    }

    public PushHandler(ChannelGroup allChannels, UUID masterUUID) throws NoSuchAlgorithmException {
        Objects.requireNonNull(allChannels);
        Objects.requireNonNull(masterUUID);
        this.allChannels = allChannels;
        this.masterUUID = masterUUID;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof AcknowledgedNeighboursEvent) {
            handleAcknowledgedNeighboursEvent((AcknowledgedNeighboursEvent) evt);
        } else if (evt instanceof PushEvent) {
            handlePushEvent((PushEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }
}
