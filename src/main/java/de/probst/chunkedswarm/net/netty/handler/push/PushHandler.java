package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushEvent;
import de.probst.chunkedswarm.net.netty.handler.push.message.PushMessage;
import de.probst.chunkedswarm.util.Block;
import de.probst.chunkedswarm.util.Graph;
import de.probst.chunkedswarm.util.NodeGroup;
import de.probst.chunkedswarm.util.NodeGroups;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelMatcher;

import java.util.HashMap;
import java.util.LinkedHashSet;
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

    // All channels
    private final ChannelGroup allChannels;

    // The master uuid, so nobody can choose this uuid
    private final UUID masterUUID;

    // Used to store all incoming events
    private final Map<UUID, AcknowledgedNeighboursEvent> acknowledgedNeighbours = new HashMap<>();

    private final Set<ChannelGroupFuture> pendingPushes = new LinkedHashSet<>();

    // The channel context
    private ChannelHandlerContext ctx;

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

        // Compute the meshes
        return outboundGraph.findMeshes(masterUUID, inboundGraph);
    }

    private ChannelMatcher nodeGroupToChannelMatcher(NodeGroup<UUID> nodeGroup) {
        return nodeGroup.getNodes()
                        .stream()
                        .map(acknowledgedNeighbours::get)
                        .map(AcknowledgedNeighboursEvent::getChannel)
                        .collect(Collectors.toSet())::contains;
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
            System.out.println("[PushHandler] Nothing to push, node group empty");
            return;
        }

        // Choose the largest group by default
        NodeGroup<UUID> largestGroup = meshes.getGroups().get(0);

        // Create chunk map from largest node group
        Map<Channel, Integer> chunkMap = nodeGroupToChunkMap(largestGroup);

        // Create a split version of the block
        Block splitBlock = new Block(evt.getBlock(),
                                     chunkMap.keySet()
                                             .stream()
                                             .map(Channel::id)
                                             .map(ChannelId::toString)
                                             .collect(Collectors.toList()));

        // Send block to all peers
        ChannelGroupFuture pushFuture = allChannels.writeAndFlush(new PushMessage(splitBlock, chunkMap),
                                                                  chunkMap.keySet()::contains);

        // Compute statistics
        long count = StreamSupport.stream(pushFuture.spliterator(), false).count();
        System.out.println("[PushHandler] Pushing: " + evt.getBlock());

        // Keep track of future
        pendingPushes.add(pushFuture);
        pushFuture.addListener(fut -> {

            // Remove from pending
            pendingPushes.remove(pushFuture);

            // Compute statistics
            long failed = !pushFuture.isSuccess() ? StreamSupport.stream(pushFuture.cause().spliterator(), false)
                                                                 .count() : 0;
            String rate = (count - failed) + "/" + count;
            System.out.println("[PushHandler] Pushed: " + evt.getBlock() + ", Success: " + rate);

            if (!pushFuture.isSuccess()) {
                pushFuture.cause().iterator().forEachRemaining(e -> e.getValue().printStackTrace());
            }
        });
    }

    public PushHandler(ChannelGroup allChannels, UUID masterUUID) {
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
