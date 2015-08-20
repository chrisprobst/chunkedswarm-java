package de.probst.chunkedswarm.net.netty;

import de.probst.chunkedswarm.net.netty.handler.app.DistributorHandler;
import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.connection.AcknowledgeConnectionsHandler;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIdRegistrationHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import de.probst.chunkedswarm.net.netty.handler.push.PushHandler;
import de.probst.chunkedswarm.net.netty.util.CloseableChannelGroup;
import de.probst.chunkedswarm.util.SwarmIdManager;
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
import java.util.Objects;
import java.util.UUID;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class Distributor implements Closeable {

    private final SwarmIdManager swarmIdManager;
    private final UUID masterUuid;
    private final EventLoopGroup eventLoopGroup;
    private final SocketAddress socketAddress;
    private final ChannelGroup allChannels;

    // Represents the result of initialization
    private final ChannelPromise initChannelPromise;

    private ChannelFuture openForwarderAcceptChannel() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, 256)
                       .handler(new ChannelInitializer<ServerChannel>() {
                           @Override
                           protected void initChannel(ServerChannel ch) throws Exception {

                               // The parent channel is used for pushing data
                               ch.pipeline().addLast(new PushHandler(allChannels, masterUuid));
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

                               // Track all channels
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));

                               // Handle swarm id management
                               ch.pipeline().addLast(new SwarmIdRegistrationHandler(allChannels, swarmIdManager));

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
        swarmIdManager = new SwarmIdManager();
        this.eventLoopGroup = eventLoopGroup;
        this.socketAddress = socketAddress;
        allChannels = new CloseableChannelGroup(eventLoopGroup.next());

        // Create master uuid and blacklist this uuid
        swarmIdManager.blacklistUuid(masterUuid = swarmIdManager.newRandomUuid());

        // *********************************************
        // **************** Initialize *****************
        // *********************************************

        // Initialize forwarder
        ChannelFuture bindFuture = openForwarderAcceptChannel();

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
