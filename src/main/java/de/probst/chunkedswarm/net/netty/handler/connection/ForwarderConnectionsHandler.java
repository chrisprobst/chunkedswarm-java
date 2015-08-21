package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgeNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.event.NeighbourConnectionEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDCollectionEvent;
import de.probst.chunkedswarm.net.netty.handler.exception.ExceptionHandler;
import de.probst.chunkedswarm.net.netty.util.NettyUtil;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Handler sends to owner channel:
 * - NeighbourConnectionEvent
 * - AcknowledgeNeighboursEvent
 * <p>
 * Handler listens to:
 * - AcknowledgeNeighboursEvent
 * - NeighbourConnectionEvent
 * - SwarmIDCollectionEvent
 * - SwarmIDAcquisitionEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class ForwarderConnectionsHandler extends ChannelHandlerAdapter {

    public static final int MAX_FORWARDER_FRAME_SIZE = 1024 * 1024;
    public static final long ACKNOWLEDGE_INTERVAL_MS = 1000;

    // The forwarder channel group
    private final ChannelGroup channels;

    // Used to keep track of engaged forwarder channels
    private final ChannelGroup engagedForwarderChannels;

    // The event loop group of all forwarder connections
    private final EventLoopGroup eventLoopGroup;

    // The bootstrap to create forwarder connections
    private final Bootstrap bootstrap = new Bootstrap();

    // Here we track all engaged connections
    private final Map<SwarmID, Channel> engagedConnections = new HashMap<>();

    // Here we track all pending connections
    private final Map<SwarmID, ChannelFuture> pendingConnections = new HashMap<>();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The current acknowledged neighbours message
    private AcknowledgeNeighboursMessage acknowledgeNeighboursMessage = new AcknowledgeNeighboursMessage();

    // The acknowledge promise is set, when neighbours get acknowledged
    private ChannelPromise acknowledgeChannelPromise;

    private void fireAcknowledgeNeighbours() {
        ctx.pipeline().fireUserEventTriggered(new AcknowledgeNeighboursEvent());
    }

    private void scheduleFireAcknowledgeNeighbours() {
        ctx.executor().schedule(this::fireAcknowledgeNeighbours, ACKNOWLEDGE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void fireChannelConnectionRefused(SwarmID swarmID, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new NeighbourConnectionEvent(swarmID,
                                                                channel,
                                                                NeighbourConnectionEvent.Direction.Outbound,
                                                                NeighbourConnectionEvent.Type.ConnectionRefused));
    }


    private void fireChannelConnected(SwarmID swarmID, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new NeighbourConnectionEvent(swarmID,
                                                                channel,
                                                                NeighbourConnectionEvent.Direction.Outbound,
                                                                NeighbourConnectionEvent.Type.Connected));
    }

    private void fireChannelDisconnected(SwarmID swarmID, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new NeighbourConnectionEvent(swarmID,
                                                                channel,
                                                                NeighbourConnectionEvent.Direction.Outbound,
                                                                NeighbourConnectionEvent.Type.Disconnected));
    }

    private void initBootstrap(SwarmID localSwarmID) {
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<Channel>() {
                     @Override
                     protected void initChannel(Channel ch) throws Exception {
                         // Codec
                         NettyUtil.addCodecToPipeline(ch.pipeline(), MAX_FORWARDER_FRAME_SIZE);

                         // The forwarder connection handler
                         ch.pipeline().addLast(new ForwarderConnectionHandler(localSwarmID));

                         // Handle exception logic
                         ch.pipeline().addLast(new ExceptionHandler("ForwarderToCollector"));
                     }
                 });
    }

    private void connectBySwarmID(SwarmID swarmID) {
        // Do not connect to the same swarm id twice
        if (engagedConnections.containsKey(swarmID) || pendingConnections.containsKey(swarmID)) {
            return;
        }

        // Try to engage connections
        ChannelFuture connectFuture = bootstrap.connect(swarmID.getAddress());

        // Get channel
        Channel channel = connectFuture.channel();

        // Remember the pending connection attempt
        pendingConnections.put(swarmID, connectFuture);

        // Add to channel group
        channels.add(connectFuture.channel());

        // If connecting succeeds, notify us!
        connectFuture.addListener(fut -> {
            if (fut.isSuccess()) {
                // Tell channel that outbound connection is established
                fireChannelConnected(swarmID, channel);

                // Look for channel close event
                channel.closeFuture().addListener(fut2 -> fireChannelDisconnected(swarmID, channel));
            } else {
                // Tell channel that outbound connection was refused
                fireChannelConnectionRefused(swarmID, channel);
            }

            // Channel will be closed, if connecting did not succeed!
        });
    }

    private void disconnectBySwarmID(SwarmID swarmID) {
        if (engagedConnections.containsKey(swarmID)) {
            engagedConnections.get(swarmID).close();
        } else if (pendingConnections.containsKey(swarmID)) {
            pendingConnections.get(swarmID).channel().close();
        }
    }

    private void handleSwarmIDCollectionEvent(SwarmIDCollectionEvent evt) {
        // Connect & disconnect from new/missing swarm ids
        evt.getUpdateNeighboursMessage().getAddNeighbours().forEach(this::connectBySwarmID);
        evt.getUpdateNeighboursMessage().getRemoveNeighbours().forEach(this::disconnectBySwarmID);
    }

    private void handleConnectionEvent(NeighbourConnectionEvent evt) {
        switch (evt.getDirection()) {
            case Outbound:
                switch (evt.getType()) {
                    case ConnectionRefused:
                        if (!pendingConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmID())");
                        }
                        if (engagedConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException("engagedConnections.containsKey(evt.getSwarmID())");
                        }


                        // Connection refused...
                        // TODO: (major) Reconnect ?
                        pendingConnections.remove(evt.getSwarmID());

                        break;
                    case Connected:
                        if (!pendingConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmID())");
                        }
                        if (engagedConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException("engagedConnections.containsKey(evt.getSwarmID())");
                        }

                        // Connection complete, move to engaged connections
                        pendingConnections.remove(evt.getSwarmID());
                        engagedConnections.put(evt.getSwarmID(), evt.getChannel());
                        engagedForwarderChannels.add(evt.getChannel());

                        // Add to added neighbours
                        acknowledgeNeighboursMessage.getAddedOutboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                    case Disconnected:
                        if (!pendingConnections.containsKey(evt.getSwarmID()) &&
                            !engagedConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmID()) && " +
                                                            "!engagedConnections.containsKey(evt.getSwarmID())");
                        }

                        // Remove from both maps
                        pendingConnections.remove(evt.getSwarmID());
                        engagedConnections.remove(evt.getSwarmID());

                        // Add to removed neighbours
                        acknowledgeNeighboursMessage.getAddedOutboundNeighbours().remove(evt.getSwarmID().getUUID());
                        acknowledgeNeighboursMessage.getRemovedOutboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                }
                break;
            case Inbound:
                switch (evt.getType()) {
                    case ConnectionRefused:

                        // TODO: (major) What could this mean ? Maybe that WE refused a connection ?
                        break;
                    case Connected:
                        // TODO: (major) Verify SwarmID !!
                        // Add to added neighbours
                        acknowledgeNeighboursMessage.getAddedInboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                    case Disconnected:
                        // Add to removed neighbours
                        acknowledgeNeighboursMessage.getAddedInboundNeighbours().remove(evt.getSwarmID().getUUID());
                        acknowledgeNeighboursMessage.getRemovedInboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                }
        }
    }

    private void handleSwarmIDAcquisitionEvent(SwarmIDAcquisitionEvent evt) {
        // Initialize bootstrap with local swarm if
        initBootstrap(evt.getSwarmID());
    }

    private void handleAcknowledgeNeighboursEvent(AcknowledgeNeighboursEvent evt) {
        acknowledgeNeighbours();
        scheduleFireAcknowledgeNeighbours();
    }

    private void acknowledgeNeighbours() {
        // No updates, skip this update
        if (acknowledgeNeighboursMessage.isEmpty()) {
            return;
        }

        // Make sure, the message is valid
        if (!acknowledgeNeighboursMessage.isOutboundDistinct() || !acknowledgeNeighboursMessage.isInboundDistinct()) {
            // Send to channel
            ctx.channel()
               .pipeline()
               .fireExceptionCaught(new IllegalStateException("!acknowledgeNeighboursMessage.isOutboundDistinct() || " +
                                                              "!acknowledgeNeighboursMessage.isInboundDistinct()"));
        }

        // Pending acknowledge channel future, just ignore the current update
        if (acknowledgeChannelPromise != null) {
            return;
        }

        // Prepare promise
        acknowledgeChannelPromise = ctx.newPromise();

        // Write the update neighbours message
        ctx.writeAndFlush(acknowledgeNeighboursMessage, acknowledgeChannelPromise)
           .addListener(fut -> acknowledgeChannelPromise = null)
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);


        // Create a new acknowledge message
        acknowledgeNeighboursMessage = new AcknowledgeNeighboursMessage();
    }

    public ForwarderConnectionsHandler(ChannelGroup channels,
                                       ChannelGroup engagedForwarderChannels,
                                       EventLoopGroup eventLoopGroup) {
        Objects.requireNonNull(channels);
        Objects.requireNonNull(eventLoopGroup);
        this.channels = channels;
        this.eventLoopGroup = eventLoopGroup;
        this.engagedForwarderChannels = engagedForwarderChannels;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIDCollectionEvent) {
            handleSwarmIDCollectionEvent((SwarmIDCollectionEvent) evt);
        } else if (evt instanceof NeighbourConnectionEvent) {
            handleConnectionEvent((NeighbourConnectionEvent) evt);
        } else if (evt instanceof SwarmIDAcquisitionEvent) {
            handleSwarmIDAcquisitionEvent((SwarmIDAcquisitionEvent) evt);
        } else if (evt instanceof AcknowledgeNeighboursEvent) {
            handleAcknowledgeNeighboursEvent((AcknowledgeNeighboursEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        scheduleFireAcknowledgeNeighbours();
        super.channelActive(ctx);
    }
}
