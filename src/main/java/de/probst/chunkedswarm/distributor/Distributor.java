package de.probst.chunkedswarm.distributor;

import de.probst.chunkedswarm.net.netty.handler.app.DistributorHandler;
import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIdRegistrationHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import de.probst.chunkedswarm.util.SwarmIdManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class Distributor implements Closeable {

    private static final int distributorPort = 1337;

    private final EventLoopGroup eventLoopGroup;
    private final ServerChannel serverChannel;
    private final ChannelGroup channelGroup;

    private ServerChannel startDistributor() throws InterruptedException {
        SwarmIdManager swarmIdManager = new SwarmIdManager();

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, 256)
                       .childOption(ChannelOption.TCP_NODELAY, true)
                       .childHandler(new ChannelInitializer<SocketChannel>() {
                           @Override
                           protected void initChannel(SocketChannel ch) throws Exception {
                               // Codec
                               ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                               ch.pipeline().addLast(new LengthFieldPrepender(4));
                               ch.pipeline().addLast(new SimpleCodec());

                               // Track all channels
                               ch.pipeline().addLast(new ChannelGroupHandler(channelGroup));

                               // Handle swarm id management
                               ch.pipeline().addLast(new SwarmIdRegistrationHandler(swarmIdManager));

                               // Handle application logic
                               ch.pipeline().addLast(new DistributorHandler());
                           }
                       });
        return (ServerChannel) serverBootstrap.bind(distributorPort).sync().channel();
    }

    public Distributor(EventLoopGroup eventLoopGroup) throws InterruptedException {
        Objects.requireNonNull(eventLoopGroup);
        this.eventLoopGroup = eventLoopGroup;
        channelGroup = new DefaultChannelGroup(eventLoopGroup.next());
        serverChannel = startDistributor();
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public ServerChannel getServerChannel() {
        return serverChannel;
    }

    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }

    @Override
    public void close() throws IOException {
        try {
            channelGroup.close().sync();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
