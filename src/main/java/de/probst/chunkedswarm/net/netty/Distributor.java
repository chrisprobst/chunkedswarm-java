package de.probst.chunkedswarm.net.netty;

import de.probst.chunkedswarm.net.netty.handler.app.DistributorHandler;
import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.connection.AcknowledgeConnectionsHandler;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIDRegistrationHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import de.probst.chunkedswarm.net.netty.handler.push.PushHandler;
import de.probst.chunkedswarm.net.netty.handler.push.PushWriteRequestHandler;
import de.probst.chunkedswarm.net.netty.handler.push.event.PushEvent;
import de.probst.chunkedswarm.net.netty.util.CloseableChannelGroup;
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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

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
public final class Distributor implements Closeable {

    public static final int BACKLOG = 256;

    private final SwarmIDManager swarmIDManager;
    private final UUID masterUUID;
    private final EventLoopGroup acceptEventLoopGroup;
    private final EventLoopGroup eventLoopGroup;
    private final SocketAddress socketAddress;
    private final ChannelGroup allChannels;
    private final Channel acceptorChannel;

    // Represents the result of initialization
    private final ChannelPromise initChannelPromise;

    private ChannelFuture openForwarderAcceptChannel() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptEventLoopGroup, eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, BACKLOG)
                       .handler(new ChannelInitializer<ServerChannel>() {
                           @Override
                           protected void initChannel(ServerChannel ch) throws Exception {

                               // The parent channel is used for pushing data
                               ch.pipeline().addLast(new PushHandler(allChannels, masterUUID));
                           }
                       })
                       .childOption(ChannelOption.TCP_NODELAY, true)
                       .childHandler(new ChannelInitializer<Channel>() {
                           @Override
                           protected void initChannel(Channel ch) throws Exception {
                               // Codec
                               ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                               ch.pipeline().addLast(new LengthFieldPrepender(4));
                               ch.pipeline().addLast(new SimpleCodec());
                               ch.pipeline().addLast(new PushWriteRequestHandler());

                               // Track all channels
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));

                               // Handle swarm id management
                               ch.pipeline().addLast(new SwarmIDRegistrationHandler(allChannels, swarmIDManager));

                               // Handle connection acknowledgements
                               ch.pipeline().addLast(new AcknowledgeConnectionsHandler());

                               // Handle application logic
                               ch.pipeline().addLast(new DistributorHandler());
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

    public Distributor(EventLoopGroup eventLoopGroup, SocketAddress socketAddress) {
        Objects.requireNonNull(eventLoopGroup);
        Objects.requireNonNull(socketAddress);

        // Init attributes
        swarmIDManager = new SwarmIDManager();
        this.eventLoopGroup = eventLoopGroup;
        this.socketAddress = socketAddress;

        // Init accept event loop group and use it with channel group
        acceptEventLoopGroup = eventLoopGroup.next();
        allChannels = new CloseableChannelGroup(acceptEventLoopGroup.next());

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
        acceptorChannel.pipeline().fireUserEventTriggered(new PushEvent(payload, sequence, priority, duration));
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
