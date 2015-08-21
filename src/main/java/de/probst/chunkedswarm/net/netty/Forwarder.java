package de.probst.chunkedswarm.net.netty;

import de.probst.chunkedswarm.net.netty.handler.connection.CollectorConnectionHandler;
import de.probst.chunkedswarm.net.netty.handler.connection.ForwarderConnectionsHandler;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIDCollectionHandler;
import de.probst.chunkedswarm.net.netty.handler.exception.ExceptionHandler;
import de.probst.chunkedswarm.net.netty.handler.forwarding.ForwardingHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import de.probst.chunkedswarm.net.netty.util.CloseableChannelGroup;
import de.probst.chunkedswarm.net.netty.util.NettyUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class Forwarder implements Closeable {

    public static final int MAX_DISTRIBUTOR_FRAME_SIZE = 1024 * 1024 * 30;
    public static final int MAX_COLLECTOR_FRAME_SIZE = 1024 * 1024 * 10;
    public static final int BACKLOG = 256;

    private final EventLoopGroup eventLoopGroup;
    private final SocketAddress collectorAcceptorAddress;
    private final Channel distributorChannel;
    private final ChannelGroup collectorChannels, engagedForwarderChannels, allChannels;

    // Represents the result of initialization
    private final ChannelPromise initChannelPromise;

    private ChannelFuture openCollectorAcceptChannel() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, BACKLOG)
                       .childOption(ChannelOption.TCP_NODELAY, true)
                       .childHandler(new ChannelInitializer<Channel>() {
                           @Override
                           protected void initChannel(Channel ch) throws Exception {
                               // Codec
                               NettyUtil.addCodecToPipeline(ch.pipeline(), MAX_COLLECTOR_FRAME_SIZE);

                               // Track all channels
                               ch.pipeline().addLast(new ChannelGroupHandler(collectorChannels));
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));

                               // Add the collector connections tracker
                               ch.pipeline().addLast(new CollectorConnectionHandler(distributorChannel));

                               // Handle exception logic
                               ch.pipeline().addLast(new ExceptionHandler("CollectorToForwarder"));
                           }
                       });

        // Open collector accept channel
        ChannelFuture bindFuture = serverBootstrap.bind(collectorAcceptorAddress);

        // Add to channel groups
        collectorChannels.add(bindFuture.channel());
        allChannels.add(bindFuture.channel());

        // Close the forwarder, if the collector is closed
        bindFuture.channel().closeFuture().addListener(fut -> closeAsync());

        return bindFuture;
    }

    private ChannelFuture connectToDistributor(SocketAddress socketAddress) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<Channel>() {
                     @Override
                     protected void initChannel(Channel ch) throws Exception {

                         // Codec
                         NettyUtil.addCodecToPipeline(ch.pipeline(), MAX_DISTRIBUTOR_FRAME_SIZE);

                         // Handle swarm id management
                         ch.pipeline().addLast(new SwarmIDCollectionHandler(collectorAcceptorAddress));

                         // Handle forwarder connections based on swarm id collections
                         ch.pipeline()
                           .addLast(new ForwarderConnectionsHandler(allChannels,
                                                                    engagedForwarderChannels,
                                                                    eventLoopGroup));

                         // Handle push messages, by forwarding them to all forwarder channels
                         ch.pipeline().addLast(new ForwardingHandler(engagedForwarderChannels));

                         // Handle exception logic
                         ch.pipeline().addLast(new ExceptionHandler("ForwarderToDistributor"));
                     }
                 });

        // Connect and store channel future
        ChannelFuture connectFuture = bootstrap.connect(socketAddress);

        // Add to channel group
        allChannels.add(connectFuture.channel());

        // Close the forwarder, if connection to distributor is lost
        connectFuture.channel().closeFuture().addListener(fut -> closeAsync());

        return connectFuture;
    }

    public Forwarder(EventLoopGroup eventLoopGroup,
                     SocketAddress collectorAcceptorAddress,
                     SocketAddress distributorAddress) {
        Objects.requireNonNull(eventLoopGroup);
        Objects.requireNonNull(collectorAcceptorAddress);
        Objects.requireNonNull(distributorAddress);

        // Init attributes
        this.eventLoopGroup = eventLoopGroup;
        this.collectorAcceptorAddress = collectorAcceptorAddress;
        collectorChannels = new DefaultChannelGroup(eventLoopGroup.next());
        engagedForwarderChannels = new DefaultChannelGroup(eventLoopGroup.next());
        allChannels = new CloseableChannelGroup(eventLoopGroup.next());

        // *********************************************
        // **************** Initialize *****************
        // *********************************************

        // Initialize connection to distributor
        ChannelFuture connectFuture = connectToDistributor(distributorAddress);

        // Store distributor channel
        distributorChannel = connectFuture.channel();

        // Create new init promise
        initChannelPromise = distributorChannel.newPromise();

        // Listen for connection
        connectFuture.addListener(fut -> {

            // Check for connect success
            if (!fut.isSuccess()) {

                // Stop initialization here
                initChannelPromise.setFailure(fut.cause());
            } else {

                // Initialize collector accept channel and listen for bind
                openCollectorAcceptChannel().addListener(fut2 -> {

                    // Check for connect success
                    if (!fut2.isSuccess()) {

                        // Close channel...
                        distributorChannel.close();

                        // Stop initialization here
                        initChannelPromise.setFailure(fut2.cause());
                    } else {

                        // Set init success
                        initChannelPromise.setSuccess();
                    }
                });
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
