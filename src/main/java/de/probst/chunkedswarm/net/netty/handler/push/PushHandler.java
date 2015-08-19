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
import java.util.concurrent.TimeUnit;
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

    public static final long PUSH_INTERVAL_MS = 1000;

    // All channels
    private final ChannelGroup allChannels;

    // The master uuid, so nobody can choose this id
    private final String masterUuid;

    // Used to store all incoming events
    private final Map<String, AcknowledgedNeighboursEvent> acknowledgedNeighbours = new HashMap<>();

    // The channel context
    private ChannelHandlerContext ctx;

    private void firePush() {
        ctx.pipeline().fireUserEventTriggered(new PushEvent());
    }

    private void scheduleFirePush() {
        ctx.executor().schedule(this::firePush, PUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private NodeGroups<String> computeMeshes() {
        // Create graphs to compute the meshes
        Graph<String> outboundGraph = new Graph<>();
        Graph<String> inboundGraph = new Graph<>();

        // The master node groups
        NodeGroup<String> masterOutboundNodeGroup = new NodeGroup<>();
        NodeGroup<String> masterInboundNodeGroup = new NodeGroup<>();

        // Create node groups for each node
        acknowledgedNeighbours.values().forEach(evt -> {
            NodeGroup<String> outboundNodeGroup = new NodeGroup<>();
            NodeGroup<String> inboundNodeGroup = new NodeGroup<>();

            // Add all acknowledged neighbours into group
            outboundNodeGroup.getNodes().addAll(evt.getAcknowledgedOutboundNeighbours());
            inboundNodeGroup.getNodes().addAll(evt.getAcknowledgedInboundNeighbours());

            // Of course, each node is connected with the master
            outboundNodeGroup.getNodes().add(masterUuid);
            inboundNodeGroup.getNodes().add(masterUuid);

            // The master is connected with this node
            masterOutboundNodeGroup.getNodes().add(evt.getLocalSwarmId().getUuid());
            masterInboundNodeGroup.getNodes().add(evt.getLocalSwarmId().getUuid());

            // Put node groups into graphs
            outboundGraph.getNodes().put(evt.getLocalSwarmId().getUuid(), outboundNodeGroup);
            inboundGraph.getNodes().put(evt.getLocalSwarmId().getUuid(), inboundNodeGroup);
        });

        // Put the view of the master into the graphs
        outboundGraph.getNodes().put(masterUuid, masterOutboundNodeGroup);
        inboundGraph.getNodes().put(masterUuid, masterInboundNodeGroup);

        // Compute the meshes
        return outboundGraph.findMeshes(masterUuid, inboundGraph);
    }

    private ChannelMatcher nodeGroupToChannelMatcher(NodeGroup<String> nodeGroup) {
        return nodeGroup.getNodes()
                        .stream()
                        .map(acknowledgedNeighbours::get)
                        .map(AcknowledgedNeighboursEvent::getChannel)
                        .collect(Collectors.toSet())::contains;
    }

    private void handleAcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent evt) {
        switch (evt.getType()) {
            case Update:
                acknowledgedNeighbours.put(evt.getLocalSwarmId().getUuid(), evt);
                break;
            case Dispose:
                acknowledgedNeighbours.remove(evt.getLocalSwarmId().getUuid());
                break;
        }
    }

    private void handlePushEvent(PushEvent evt) {
        push();
        scheduleFirePush();
    }

    int i = 0;
    volatile ChannelGroupFuture next;

    private void push() {


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
        NodeGroups<String> meshes = computeMeshes();
        if (!meshes.getGroups().isEmpty()) {
            NodeGroup<String> largestGroup = meshes.getGroups().get(0);
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

    public PushHandler(ChannelGroup allChannels, String masterUuid) {
        Objects.requireNonNull(allChannels);
        Objects.requireNonNull(masterUuid);
        this.allChannels = allChannels;
        this.masterUuid = masterUuid;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        scheduleFirePush();
        super.channelActive(ctx);
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
