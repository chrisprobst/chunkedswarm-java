package de.probst.chunkedswarm.net.netty;

import de.probst.chunkedswarm.net.netty.handler.app.DistributorHandler;
import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIdRegistrationHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import de.probst.chunkedswarm.util.SwarmIdManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
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

    private final SwarmIdManager swarmIdManager;
    private final EventLoopGroup eventLoopGroup;
    private final int port;
    private final ServerChannel forwarderAcceptorChannel;
    private final ChannelGroup forwarderChannels, allChannels;

    private ServerChannel openForwarderAcceptChannel() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, 256)
                       .handler(new ChannelInitializer<ServerChannel>() {
                           @Override
                           protected void initChannel(ServerChannel ch) throws Exception {
                               // Track all channels
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));
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
                               ch.pipeline().addLast(new ChannelGroupHandler(forwarderChannels));
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));

                               // Handle swarm id management
                               ch.pipeline().addLast(new SwarmIdRegistrationHandler(swarmIdManager));

                               // Handle application logic
                               ch.pipeline().addLast(new DistributorHandler());
                           }
                       });
        return (ServerChannel) serverBootstrap.bind(port).syncUninterruptibly().channel();
    }

    public Distributor(EventLoopGroup eventLoopGroup, int port) {
        Objects.requireNonNull(eventLoopGroup);
        swarmIdManager = new SwarmIdManager();
        this.eventLoopGroup = eventLoopGroup;
        this.port = port;
        forwarderChannels = new DefaultChannelGroup(eventLoopGroup.next());
        allChannels = new DefaultChannelGroup(eventLoopGroup.next());
        forwarderAcceptorChannel = openForwarderAcceptChannel();
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public int getPort() {
        return port;
    }

    public ServerChannel getForwarderAcceptorChannel() {
        return forwarderAcceptorChannel;
    }

    public ChannelGroup getForwarderChannels() {
        return forwarderChannels;
    }

    public ChannelGroup getAllChannels() {
        return allChannels;
    }

    @Override
    public void close() throws IOException {
        try {
            allChannels.close().sync();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
