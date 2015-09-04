package de.probst.chunkedswarm.net.netty.app;

import de.probst.chunkedswarm.net.netty.handler.connection.AcknowledgeConnectionsHandler;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIDRegistrationHandler;
import de.probst.chunkedswarm.net.netty.handler.exception.ExceptionHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import de.probst.chunkedswarm.net.netty.handler.push.PushHandler;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushRequestEvent;
import de.probst.chunkedswarm.net.netty.util.CloseableChannelGroup;
import de.probst.chunkedswarm.net.netty.util.NettyUtil;
import de.probst.chunkedswarm.util.SwarmIDManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class NettyDistributor implements Closeable {

    public static final int MAX_FORWARDER_FRAME_SIZE = 1024 * 1024 * 100;
    public static final int BACKLOG = 256;

    private final SwarmIDManager swarmIDManager;
    private final UUID masterUUID;
    private final EventLoopGroup bossEventLoopGroup;
    private final EventLoopGroup eventLoopGroup;
    private final SocketAddress socketAddress;
    private final ChannelGroup allChannels;
    private final Channel acceptorChannel;

    // Represents the result of initialization
    private final ChannelPromise initChannelPromise;

    private ChannelFuture openForwarderAcceptChannel() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossEventLoopGroup, eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, BACKLOG)
                       .handler(new ChannelInitializer<ServerChannel>() {
                           @Override
                           protected void initChannel(ServerChannel ch) throws Exception {

                               // The parent channel is used for pushing data
                               ch.pipeline().addLast(new PushHandler(masterUUID));

                               // Handle exception logic
                               ch.pipeline().addLast(new ExceptionHandler("DistributorAcceptor"));
                           }
                       })
                       .childOption(ChannelOption.TCP_NODELAY, true)
                       .childHandler(new ChannelInitializer<Channel>() {
                           @Override
                           protected void initChannel(Channel ch) throws Exception {
                               // Codec
                               NettyUtil.addCodecToPipeline(ch.pipeline(), MAX_FORWARDER_FRAME_SIZE);

                               // Track all channels
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));

                               // Handle swarm id management
                               ch.pipeline().addLast(new SwarmIDRegistrationHandler(allChannels, swarmIDManager));

                               // Handle connection acknowledgements
                               ch.pipeline().addLast(new AcknowledgeConnectionsHandler());

                               // Handle exception logic
                               ch.pipeline().addLast(new ExceptionHandler("DistributorToForwarder"));
                           }
                       });

        // Open forwarder accept channel
        ChannelFuture bindFuture = serverBootstrap.bind(socketAddress);

        // Add to channel group
        allChannels.add(bindFuture.channel());

        // Close the distributor, if server socket is closed!
        bindFuture.channel().closeFuture().addListener(fut -> closeAsync());

        return bindFuture;
    }

    public NettyDistributor(EventLoopGroup bossEventLoopGroup,
                            EventLoopGroup eventLoopGroup,
                            SocketAddress socketAddress) {
        Objects.requireNonNull(bossEventLoopGroup);
        Objects.requireNonNull(eventLoopGroup);
        Objects.requireNonNull(socketAddress);

        // Init attributes
        swarmIDManager = new SwarmIDManager();
        this.bossEventLoopGroup = bossEventLoopGroup;
        this.eventLoopGroup = eventLoopGroup;
        this.socketAddress = socketAddress;
        allChannels = new CloseableChannelGroup(eventLoopGroup.next());

        // Create master uuid and blacklist this uuid
        swarmIDManager.blacklistUUID(masterUUID = swarmIDManager.newRandomUUID());

        // *********************************************
        // **************** Initialize *****************
        // *********************************************

        // Initialize forwarder
        ChannelFuture bindFuture = openForwarderAcceptChannel();

        // Save acceptor channel
        acceptorChannel = bindFuture.channel();

        // Create new init promise
        initChannelPromise = bindFuture.channel().newPromise();

        // Listen for bind
        bindFuture.addListener(fut -> {

            // Check for bind success
            if (!fut.isSuccess()) {

                // Stop initialization here
                initChannelPromise.setFailure(fut.cause());
            } else {

                // Set init success
                initChannelPromise.setSuccess();
            }
        });
    }

    public ChannelFuture getInitFuture() {
        return initChannelPromise;
    }

    public void distribute(ByteBuffer payload, int sequence, int priority, Duration duration) {
        acceptorChannel.pipeline().fireUserEventTriggered(new PushRequestEvent(payload, sequence, priority, duration));
    }

    public ChannelGroupFuture closeAsync() {
        return allChannels.close();
    }

    @Override
    public void close() throws IOException {
        try {
            closeAsync().syncUninterruptibly();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
