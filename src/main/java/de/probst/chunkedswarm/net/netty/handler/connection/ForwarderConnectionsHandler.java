package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgeNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.event.NeighbourConnectionEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdCollectionEvent;
import de.probst.chunkedswarm.util.SwarmId;
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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

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
 * - SwarmIdCollectionEvent
 * - SwarmIdAcquisitionEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class ForwarderConnectionsHandler extends ChannelHandlerAdapter {

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
    private final Map<SwarmId, Channel> engagedConnections = new HashMap<>();

    // Here we track all pending connections
    private final Map<SwarmId, ChannelFuture> pendingConnections = new HashMap<>();

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

    private void fireChannelConnectionRefused(SwarmId swarmId, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new NeighbourConnectionEvent(swarmId,
                                                                channel,
                                                                NeighbourConnectionEvent.Direction.Outbound,
                                                                NeighbourConnectionEvent.Type.ConnectionRefused));
    }


    private void fireChannelConnected(SwarmId swarmId, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new NeighbourConnectionEvent(swarmId,
                                                                channel,
                                                                NeighbourConnectionEvent.Direction.Outbound,
                                                                NeighbourConnectionEvent.Type.Connected));
    }

    private void fireChannelDisconnected(SwarmId swarmId, Channel channel) {
        ctx.pipeline()
           .fireUserEventTriggered(new NeighbourConnectionEvent(swarmId,
                                                                channel,
                                                                NeighbourConnectionEvent.Direction.Outbound,
                                                                NeighbourConnectionEvent.Type.Disconnected));
    }

    private void initBootstrap(SwarmId localSwarmId) {
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<Channel>() {
                     @Override
                     protected void initChannel(Channel ch) throws Exception {

                         // Codec
                         ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                         ch.pipeline().addLast(new LengthFieldPrepender(4));
                         ch.pipeline().addLast(new SimpleCodec());

                         // The forwarder connection handler
                         ch.pipeline().addLast(new ForwarderConnectionHandler(localSwarmId));
                     }
                 });
    }

    private void connectBySwarmId(SwarmId swarmId) {
        // Do not connect to the same swarm id twice
        if (engagedConnections.containsKey(swarmId) || pendingConnections.containsKey(swarmId)) {
            return;
        }

        // Try to engage connections
        ChannelFuture connectFuture = bootstrap.connect(swarmId.getAddress());

        // Get channel
        Channel channel = connectFuture.channel();

        // Remember the pending connection attempt
        pendingConnections.put(swarmId, connectFuture);

        // Add to channel group
        channels.add(connectFuture.channel());

        // If connecting succeeds, notify us!
        connectFuture.addListener(fut -> {
            if (fut.isSuccess()) {
                // Tell channel that outbound connection is established
                fireChannelConnected(swarmId, channel);

                // Look for channel close event
                channel.closeFuture().addListener(fut2 -> fireChannelDisconnected(swarmId, channel));
            } else {
                // Tell channel that outbound connection was refused
                fireChannelConnectionRefused(swarmId, channel);
            }

            // Channel will be closed, if connecting did not succeed!
        });
    }

    private void disconnectBySwarmId(SwarmId swarmId) {
        if (engagedConnections.containsKey(swarmId)) {
            engagedConnections.get(swarmId).close();
        } else if (pendingConnections.containsKey(swarmId)) {
            pendingConnections.get(swarmId).channel().close();
        }
    }

    private void handleSwarmIdCollectionEvent(SwarmIdCollectionEvent evt) {
        // Connect & disconnect from new/missing swarm ids
        evt.getUpdateNeighboursMessage().getAddNeighbours().forEach(this::connectBySwarmId);
        evt.getUpdateNeighboursMessage().getRemoveNeighbours().forEach(this::disconnectBySwarmId);
    }

    private void handleConnectionEvent(NeighbourConnectionEvent evt) {
        switch (evt.getDirection()) {
            case Outbound:
                switch (evt.getType()) {
                    case ConnectionRefused:
                        if (!pendingConnections.containsKey(evt.getSwarmId())) {
                            throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmId())");
                        }
                        if (engagedConnections.containsKey(evt.getSwarmId())) {
                            throw new IllegalStateException("engagedConnections.containsKey(evt.getSwarmId())");
                        }


                        // Connection refused...
                        // TODO: Reconnect ?
                        pendingConnections.remove(evt.getSwarmId());

                        break;
                    case Connected:
                        if (!pendingConnections.containsKey(evt.getSwarmId())) {
                            throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmId())");
                        }
                        if (engagedConnections.containsKey(evt.getSwarmId())) {
                            throw new IllegalStateException("engagedConnections.containsKey(evt.getSwarmId())");
                        }

                        // Connection complete, move to engaged connections
                        pendingConnections.remove(evt.getSwarmId());
                        engagedConnections.put(evt.getSwarmId(), evt.getChannel());
                        engagedForwarderChannels.add(evt.getChannel());

                        // Add to added neighbours
                        acknowledgeNeighboursMessage.getAddedOutboundNeighbours().add(evt.getSwarmId().getUuid());
                        break;
                    case Disconnected:
                        if (!pendingConnections.containsKey(evt.getSwarmId()) &&
                            !engagedConnections.containsKey(evt.getSwarmId())) {
                            throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmId()) && " +
                                                            "!engagedConnections.containsKey(evt.getSwarmId())");
                        }

                        // Remove from both maps
                        pendingConnections.remove(evt.getSwarmId());
                        engagedConnections.remove(evt.getSwarmId());

                        // Add to removed neighbours
                        acknowledgeNeighboursMessage.getAddedOutboundNeighbours().remove(evt.getSwarmId().getUuid());
                        acknowledgeNeighboursMessage.getRemovedOutboundNeighbours().add(evt.getSwarmId().getUuid());
                        break;
                }
                break;
            case Inbound:
                switch (evt.getType()) {
                    case ConnectionRefused:

                        // TODO: What could this mean ? Maybe that WE refused a connection ?
                        break;
                    case Connected:
                        // Add to added neighbours
                        acknowledgeNeighboursMessage.getAddedInboundNeighbours().add(evt.getSwarmId().getUuid());
                        break;
                    case Disconnected:
                        // Add to removed neighbours
                        acknowledgeNeighboursMessage.getAddedInboundNeighbours().remove(evt.getSwarmId().getUuid());
                        acknowledgeNeighboursMessage.getRemovedInboundNeighbours().add(evt.getSwarmId().getUuid());
                        break;
                }
        }
    }

    private void handleSwarmIdAcquisitionEvent(SwarmIdAcquisitionEvent evt) {
        // Initialize bootstrap with local swarm if
        initBootstrap(evt.getSwarmId());
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
        if (evt instanceof SwarmIdCollectionEvent) {
            handleSwarmIdCollectionEvent((SwarmIdCollectionEvent) evt);
        } else if (evt instanceof NeighbourConnectionEvent) {
            handleConnectionEvent((NeighbourConnectionEvent) evt);
        } else if (evt instanceof SwarmIdAcquisitionEvent) {
            handleSwarmIdAcquisitionEvent((SwarmIdAcquisitionEvent) evt);
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
