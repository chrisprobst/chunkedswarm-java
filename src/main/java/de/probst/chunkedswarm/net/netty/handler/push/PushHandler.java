package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushCompletedEvent;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushRequestEvent;
import de.probst.chunkedswarm.util.BlockHeader;
import de.probst.chunkedswarm.util.Graph;
import de.probst.chunkedswarm.util.NodeGroup;
import de.probst.chunkedswarm.util.NodeGroups;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Handler sends to owner channel:
 * - PushCompletedEvent
 * <p>
 * Handler listens to:
 * - PushRequestEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 18.08.15
 */
public final class PushHandler extends ChannelHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PushHandler.class);

    // The master uuid, so nobody can choose this uuid
    private final UUID masterUUID;

    // Used to store all incoming events
    private final Map<UUID, AcknowledgedNeighboursEvent> acknowledgedNeighbours = new HashMap<>();

    // Used to track pending pushes
    private final Set<PushTracker> pendingPushTrackers = new LinkedHashSet<>();

    // The context
    private ChannelHandlerContext ctx;

    private void firePushCompleted(PushTracker pushTracker) {
        ctx.pipeline().fireUserEventTriggered(new PushCompletedEvent(pushTracker));
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

    private NodeGroups<UUID> determinePushGroups() {

        // At first, compute all meshes
        // Default policy takes just the biggest mesh
        NodeGroups<UUID> allMeshes = computeMeshes();

        // Empty ? Return directly...
        if (allMeshes.getGroups().isEmpty()) {
            return allMeshes;
        }

        // Just consider the biggest mesh by default
        // TODO: Maybe multiple groups ?
        return new NodeGroups<>(Collections.singletonList(allMeshes.getGroups().get(0)));
    }

    private Map<Channel, Integer> nodeGroupToChunkMap(NodeGroup<UUID> nodeGroup) {
        PrimitiveIterator.OfInt idxs = IntStream.range(0, nodeGroup.getNodes().size()).iterator();
        return nodeGroup.getNodes()
                        .stream()
                        .map(acknowledgedNeighbours::get)
                        .map(AcknowledgedNeighboursEvent::getChannel)
                        .collect(Collectors.toMap(c -> c, c -> idxs.next()));
    }

    private void cancelPendingPushTrackers() {
        // TODO: Implement
    }

    private void pushGroups(PushRequestEvent evt, NodeGroups<UUID> groups) throws NoSuchAlgorithmException {
        // Nothing to push
        if (groups.getGroups().isEmpty()) {
            logger.info("Nothing to push, node group empty");
            return;
        }

        // Push each group
        for (NodeGroup<UUID> uuidNodeGroup : groups.getGroups()) {
            pushGroup(evt, uuidNodeGroup);
        }
    }

    private void pushGroup(PushRequestEvent evt, NodeGroup<UUID> group) throws NoSuchAlgorithmException {

        // Create chunk map from largest node group
        Map<Channel, Integer> chunkMap = nodeGroupToChunkMap(group);

        // Collect vars
        ByteBuffer payload = evt.getPayload().duplicate();

        // Determine chunk count
        // If less bytes than peers: Simply send whole block to every one
        // Chunk count == 1 always means no forwarding
        int chunkCount = payload.remaining() < chunkMap.size() ? 1 : chunkMap.size();

        // Create the block header
        // Very costly, invoke in thread pool
        BlockHeader blockHeader = BlockHeader.createFrom(evt.getPayload(),
                                                         evt.getSequence(),
                                                         evt.getPriority(),
                                                         evt.getDuration(),
                                                         chunkCount);

        // Send block to all peers
        PushTracker pushTracker = PushTracker.createFrom(this::firePushCompleted, blockHeader, payload, chunkMap);

        // Add the new push tracker
        pendingPushTrackers.add(pushTracker);

        // Compute statistics
        logger.info("Pushing: " + pushTracker.getBlockHeader());

    }

    private void handleAcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent evt) {
        switch (evt.getType()) {
            case Register:
            case Update:
                acknowledgedNeighbours.put(evt.getLocalSwarmID().getUUID(), evt);
                break;
            case Unregister:
                acknowledgedNeighbours.remove(evt.getLocalSwarmID().getUUID());
                break;
        }
    }

    private void handlePushRequestEvent(PushRequestEvent evt) throws NoSuchAlgorithmException {
        // Determine push groups and push
        pushGroups(evt, determinePushGroups());
    }

    private void handlePushCompletedEvent(PushCompletedEvent evt) {
        // Remove the push tracker, it is not pending anymore
        PushTracker pushTracker = evt.getPushTracker();
        pendingPushTrackers.remove(pushTracker);

        // Compute statistics
        long count = pushTracker.getChannels().size();

        // Compute statistics
        long failed = pushTracker.getFailedChannels().size();

        String rate = (count - failed) + "/" + count;
        logger.info("Pushed: " + pushTracker.getBlockHeader() + ", Success: " + rate);

        // Log failed channels
        pushTracker.getFailedChannels().forEach((c, f) -> logger.warn("Partial pushTracker failure", f.cause()));
    }

    public PushHandler(UUID masterUUID) {
        Objects.requireNonNull(masterUUID);
        this.masterUUID = masterUUID;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelPendingPushTrackers();
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof AcknowledgedNeighboursEvent) {
            handleAcknowledgedNeighboursEvent((AcknowledgedNeighboursEvent) evt);
        } else if (evt instanceof PushRequestEvent) {
            handlePushRequestEvent((PushRequestEvent) evt);
        } else if (evt instanceof PushCompletedEvent) {
            handlePushCompletedEvent((PushCompletedEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }
}
