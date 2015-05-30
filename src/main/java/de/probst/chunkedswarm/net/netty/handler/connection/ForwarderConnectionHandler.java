package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.connection.event.ConnectionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdCollectionEvent;
import de.probst.chunkedswarm.util.SwarmId;
import de.probst.chunkedswarm.util.SwarmIdSet;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class ForwarderConnectionHandler extends ChannelHandlerAdapter {

    // The forwarder channel group
    private final ChannelGroup forwarderChannels;

    // The event loop group of all forwarder connections
    private final EventLoopGroup eventLoopGroup;

    // Used to keep track of engages forwarder channels
    private final ChannelGroup engagedForwarderChannels;

    // The bootstrap to create forwarder connections
    private final Bootstrap bootstrap = new Bootstrap();

    // Here we track all engaged connections
    private final Map<SwarmId, Channel> engagedConnections = new HashMap<>();

    // Here we track all pending connections
    private final Map<SwarmId, ChannelFuture> pendingConnections = new HashMap<>();

    // The last swarm id set
    private SwarmIdSet lastSwarmIdSet;

    // The channel handler context
    private ChannelHandlerContext ctx;

    private void fireChannelConnected(SwarmId swarmId, Channel channel) {
        ctx.pipeline().fireUserEventTriggered(new ConnectionEvent(swarmId, channel, ConnectionEvent.Type.Connected));
    }

    private void fireChannelDisconnected(SwarmId swarmId, Channel channel) {
        ctx.pipeline().fireUserEventTriggered(new ConnectionEvent(swarmId, channel, ConnectionEvent.Type.Disconnected));
    }

    private void initBootstrap() {
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
        forwarderChannels.add(connectFuture.channel());

        // Let this handler know, if a channel got disconnected
        connectFuture.channel().closeFuture().addListener(fut -> fireChannelDisconnected(swarmId, channel));

        // If connecting succeeds, notify us!
        connectFuture.addListener(fut -> {
            if (fut.isSuccess()) {
                fireChannelConnected(swarmId, channel);
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
        // Evaluate what's new and whats deprecated
        SwarmIdSet swarmIdSet = evt.getSwarmIdSet(), additional, missing;
        if (lastSwarmIdSet != null) {
            additional = lastSwarmIdSet.computeAdditional(swarmIdSet);
            missing = lastSwarmIdSet.computeMissing(swarmIdSet);
        } else {
            additional = swarmIdSet;
            missing = new SwarmIdSet();
        }

        // Connect & disconnect from new/missing swarm ids
        additional.get().forEach(this::connectBySwarmId);
        missing.get().forEach(this::disconnectBySwarmId);

        // Remember the last swarm id
        lastSwarmIdSet = swarmIdSet;
    }

    private void handleConnectionEvent(ConnectionEvent evt) {
        switch (evt.getType()) {
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
                break;
            case Disconnected:
                if (!pendingConnections.containsKey(evt.getSwarmId()) &&
                    !engagedConnections.containsKey(evt.getSwarmId())) {
                    throw new IllegalStateException("!pendingConnections.containsKey(evt.getSwarmId()) && " +
                                                    "!engagedConnections.containsKey(evt.getSwarmId())");
                }

                pendingConnections.remove(evt.getSwarmId());
                engagedConnections.remove(evt.getSwarmId());
                break;
        }
    }

    public ForwarderConnectionHandler(ChannelGroup forwarderChannels, EventLoopGroup eventLoopGroup) {
        Objects.requireNonNull(forwarderChannels);
        Objects.requireNonNull(eventLoopGroup);
        this.forwarderChannels = forwarderChannels;
        this.eventLoopGroup = eventLoopGroup;
        engagedForwarderChannels = new DefaultChannelGroup(eventLoopGroup.next());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdCollectionEvent) {
            handleSwarmIdCollectionEvent((SwarmIdCollectionEvent) evt);
        } else if (evt instanceof ConnectionEvent) {
            handleConnectionEvent((ConnectionEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        initBootstrap();
        super.channelActive(ctx);
    }
}
