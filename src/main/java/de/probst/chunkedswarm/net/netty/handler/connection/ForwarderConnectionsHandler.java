package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgeNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.event.ConnectionChangeEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.event.ConnectionEvent;
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
 * - ConnectionChangeEvent
 * - AcknowledgeNeighboursEvent
 * - ConnectionEvent
 * <p>
 * Handler listens to:
 * - AcknowledgeNeighboursEvent
 * - NeighbourConnectionEvent
 * - SwarmIDCollectionEvent
 * - SwarmIDAcquisitionEvent
 * - ConnectionChangeEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class ForwarderConnectionsHandler extends ChannelHandlerAdapter {

    public static final int MAX_FORWARDER_FRAME_SIZE = 1024 * 1024;
    public static final long ACKNOWLEDGE_INTERVAL_MS = 1000;

    // The forwarder channel group
    private final ChannelGroup channels;

    // The event loop group of all forwarder connections
    private final EventLoopGroup eventLoopGroup;

    // The bootstrap to create forwarder connections
    private final Bootstrap bootstrap = new Bootstrap();

    // Here we track all engaged outbound connections
    private final Map<SwarmID, Channel> engagedOutboundConnections = new HashMap<>();

    // Here we track all engaged inbound connections
    private final Map<SwarmID, Channel> engagedInboundConnections = new HashMap<>();

    // Here we track all pending connections
    private final Map<SwarmID, ChannelFuture> pendingOutboundConnections = new HashMap<>();

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

    private void fireConnectionEvent(ConnectionChangeEvent connectionChangeEvent) {
        boolean inbound = connectionChangeEvent.getDirection() == ConnectionChangeEvent.Direction.Inbound;
        Map<SwarmID, Channel> channelMap = inbound ? engagedInboundConnections : engagedOutboundConnections;
        ctx.pipeline().fireUserEventTriggered(new ConnectionEvent(connectionChangeEvent, channelMap));
    }

    private void fireOutboundConnectionRefused(SwarmID swarmID, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new ConnectionChangeEvent(swarmID,
                                                             channel,
                                                             ConnectionChangeEvent.Direction.Outbound,
                                                             ConnectionChangeEvent.Type.ConnectionRefused));
    }

    private void fireOutboundConnected(SwarmID swarmID, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new ConnectionChangeEvent(swarmID,
                                                             channel,
                                                             ConnectionChangeEvent.Direction.Outbound,
                                                             ConnectionChangeEvent.Type.Connected));
    }

    private void fireOutboundDisconnected(SwarmID swarmID, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new ConnectionChangeEvent(swarmID,
                                                             channel,
                                                             ConnectionChangeEvent.Direction.Outbound,
                                                             ConnectionChangeEvent.Type.Disconnected));
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
        if (engagedOutboundConnections.containsKey(swarmID) || pendingOutboundConnections.containsKey(swarmID)) {
            return;
        }

        // Try to engage connections
        ChannelFuture connectFuture = bootstrap.connect(swarmID.getAddress());

        // Get channel
        Channel channel = connectFuture.channel();

        // Remember the pending connection attempt
        pendingOutboundConnections.put(swarmID, connectFuture);

        // Add to channel group
        channels.add(connectFuture.channel());

        // If connecting succeeds, notify us!
        connectFuture.addListener(fut -> {
            if (fut.isSuccess()) {
                // Tell channel that outbound connection is established
                fireOutboundConnected(swarmID, channel);

                // Look for channel close event
                channel.closeFuture()
                       .addListener(fut2 -> fireOutboundDisconnected(swarmID, channel));
            } else {
                // Tell channel that outbound connection was refused
                fireOutboundConnectionRefused(swarmID, channel);
            }

            // Channel will be closed, if connecting did not succeed!
        });
    }

    private void disconnectBySwarmID(SwarmID swarmID) {
        // Disconnect outbound peer
        if (engagedOutboundConnections.containsKey(swarmID)) {
            engagedOutboundConnections.get(swarmID).close();
        } else if (pendingOutboundConnections.containsKey(swarmID)) {
            pendingOutboundConnections.get(swarmID).channel().close();
        }

        // Disconnect inbound peers
        if (engagedInboundConnections.containsKey(swarmID)) {
            engagedInboundConnections.get(swarmID).close();
        }
    }

    private void handleSwarmIDCollectionEvent(SwarmIDCollectionEvent evt) {
        // Connect & disconnect from new/missing swarm ids
        evt.getUpdateNeighboursMessage().getAddNeighbours().forEach(this::connectBySwarmID);
        evt.getUpdateNeighboursMessage().getRemoveNeighbours().forEach(this::disconnectBySwarmID);
    }

    private void handleConnectionChangeEvent(ConnectionChangeEvent evt) {
        switch (evt.getDirection()) {
            case Outbound:
                switch (evt.getType()) {
                    case ConnectionRefused:
                        if (!pendingOutboundConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException(
                                    "!pendingOutboundConnections.containsKey(evt.getSwarmID())");
                        }
                        if (engagedOutboundConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException(
                                    "engagedOutboundConnections.containsKey(evt.getSwarmID())");
                        }

                        // Connection refused...
                        // TODO: (major) Reconnect ?
                        pendingOutboundConnections.remove(evt.getSwarmID());

                        break;
                    case Connected:
                        if (!pendingOutboundConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException(
                                    "!pendingOutboundConnections.containsKey(evt.getSwarmID())");
                        }
                        if (engagedOutboundConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException(
                                    "engagedOutboundConnections.containsKey(evt.getSwarmID())");
                        }

                        // Connection complete, move to engaged connections
                        pendingOutboundConnections.remove(evt.getSwarmID());
                        engagedOutboundConnections.put(evt.getSwarmID(), evt.getChannel());

                        // Add to added neighbours
                        acknowledgeNeighboursMessage.getAddedOutboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                    case Disconnected:
                        if (!pendingOutboundConnections.containsKey(evt.getSwarmID()) &&
                            !engagedOutboundConnections.containsKey(evt.getSwarmID())) {
                            throw new IllegalStateException(
                                    "!pendingOutboundConnections.containsKey(evt.getSwarmID()) && " +
                                    "!engagedOutboundConnections.containsKey(evt.getSwarmID())");
                        }

                        // Remove from both maps
                        pendingOutboundConnections.remove(evt.getSwarmID());
                        engagedOutboundConnections.remove(evt.getSwarmID());

                        // Add to removed neighbours
                        acknowledgeNeighboursMessage.getAddedOutboundNeighbours().remove(evt.getSwarmID().getUUID());
                        acknowledgeNeighboursMessage.getRemovedOutboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                }
                break;
            case Inbound:
                switch (evt.getType()) {
                    case Connected:
                        // TODO: (major) Verify SwarmID !!

                        // Add to inbound
                        engagedInboundConnections.put(evt.getSwarmID(), evt.getChannel());

                        // Add to added neighbours
                        acknowledgeNeighboursMessage.getAddedInboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                    case Disconnected:
                        // Remove from inbound
                        engagedInboundConnections.remove(evt.getSwarmID());

                        // Add to removed neighbours
                        acknowledgeNeighboursMessage.getAddedInboundNeighbours().remove(evt.getSwarmID().getUUID());
                        acknowledgeNeighboursMessage.getRemovedInboundNeighbours().add(evt.getSwarmID().getUUID());
                        break;
                }
                break;
        }

        // Finally fire connection event
        fireConnectionEvent(evt);
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
                                       EventLoopGroup eventLoopGroup) {
        Objects.requireNonNull(channels);
        Objects.requireNonNull(eventLoopGroup);
        this.channels = channels;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIDCollectionEvent) {
            handleSwarmIDCollectionEvent((SwarmIDCollectionEvent) evt);
        } else if (evt instanceof ConnectionChangeEvent) {
            handleConnectionChangeEvent((ConnectionChangeEvent) evt);
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
