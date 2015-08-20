package de.probst.chunkedswarm.net.netty.handler.push;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushEvent;
import de.probst.chunkedswarm.util.Graph;
import de.probst.chunkedswarm.util.NodeGroup;
import de.probst.chunkedswarm.util.NodeGroups;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelMatcher;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
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

    int i = 0;
    volatile ChannelGroupFuture next;

    private void handlePushEvent(PushEvent evt) {

        if (next != null) {
            System.out.println("Pusher: Skipping...");
            return;
        }

        ByteBuf buf = ctx.alloc().buffer(0xFFFF * 4 * 5);
        for (int j = 0; j < 0xFFFF * 5; j++) {
            buf.writeInt((int) (Math.random() * 10000));
        }
        buf.setInt(0, i++);
        int c = buf.readableBytes();
        NodeGroups<UUID> meshes = computeMeshes();
        if (!meshes.getGroups().isEmpty()) {
            NodeGroup<UUID> largestGroup = meshes.getGroups().get(0);
            System.out.println("Pusher: Pushing to node group of size: " + largestGroup.getNodes().size());
            (next = allChannels.writeAndFlush(buf, nodeGroupToChannelMatcher(largestGroup)))
                    .addListener(fut -> {
                        ChannelGroupFuture groupFut = (ChannelGroupFuture) fut;
                        long count = StreamSupport.stream(groupFut.spliterator(), false).count();
                        long failed = !groupFut.isSuccess() ? StreamSupport.stream(groupFut.cause().spliterator(),
                                                                                   false).count() : 0;
                        String rate = (count - failed) + "/" + count;

                        System.out.println("Pusher: Rate: " + rate + " Size:" + (c * count / 1024.0 / 1024.0));
                        next = null;
                    });

        } else {
            System.out.println("Pusher: Node group empty");
        }
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
