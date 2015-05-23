package de.probst.chunkedswarm.net.netty;

import de.probst.chunkedswarm.net.netty.handler.app.ForwarderHandler;
import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIdCollectionHandler;
import de.probst.chunkedswarm.net.netty.handler.group.ChannelGroupHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class Forwarder implements Closeable {

    private final EventLoopGroup eventLoopGroup;
    private final SocketAddress collectorAcceptorAddress;
    private final ChannelGroup forwarderChannels, collectorChannels, allChannels;

    private void openCollectorAcceptChannel() {
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
                               ch.pipeline().addLast(new ChannelGroupHandler(collectorChannels));
                               ch.pipeline().addLast(new ChannelGroupHandler(allChannels));
                           }
                       });
        serverBootstrap.bind(collectorAcceptorAddress).syncUninterruptibly();
    }

    private void connectToDistributor(SocketAddress socketAddress) {
        Bootstrap bootstrap = new Bootstrap();
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

                         // Track all channels
                         ch.pipeline().addLast(new ChannelGroupHandler(allChannels));

                         // Handle swarm id management
                         ch.pipeline().addLast(new SwarmIdCollectionHandler(collectorAcceptorAddress));

                         // Handle application logic
                         ch.pipeline().addLast(new ForwarderHandler());
                     }
                 });

        // Connect and return channel
        Channel channel = bootstrap.connect(socketAddress).syncUninterruptibly().channel();

        // Close the forwarder, if connection to distributor is lost!
        channel.closeFuture().addListener(fut -> close());
    }

    public Forwarder(EventLoopGroup eventLoopGroup,
                     SocketAddress collectorAcceptorAddress,
                     SocketAddress distributorAddress) {
        Objects.requireNonNull(eventLoopGroup);
        Objects.requireNonNull(collectorAcceptorAddress);
        Objects.requireNonNull(distributorAddress);
        this.eventLoopGroup = eventLoopGroup;
        this.collectorAcceptorAddress = collectorAcceptorAddress;
        forwarderChannels = new DefaultChannelGroup(eventLoopGroup.next());
        collectorChannels = new DefaultChannelGroup(eventLoopGroup.next());
        allChannels = new DefaultChannelGroup(eventLoopGroup.next());
        openCollectorAcceptChannel();
        connectToDistributor(distributorAddress);
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
