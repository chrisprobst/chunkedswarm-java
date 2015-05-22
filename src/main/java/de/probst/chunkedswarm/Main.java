package de.probst.chunkedswarm;

import de.probst.chunkedswarm.net.netty.handler.app.DistributorHandler;
import de.probst.chunkedswarm.net.netty.handler.app.ForwarderHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 19.05.15
 */
public class Main {

    private static Channel startDistributor(EventLoopGroup eventLoopGroup) throws InterruptedException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .option(ChannelOption.SO_BACKLOG, 256)
                       .handler(new LoggingHandler(LogLevel.INFO))
                       .childHandler(new ChannelInitializer<SocketChannel>() {
                           @Override
                           protected void initChannel(SocketChannel ch) throws Exception {
                               ch.pipeline().addLast(new DistributorHandler());
                           }
                       });
        return serverBootstrap.bind(1337).sync().channel();
    }

    private static List<Channel> startForwarder(EventLoopGroup eventLoopGroup) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
                         ch.pipeline().addLast(new ForwarderHandler());
                     }
                 });

        return IntStream.range(0, 10)
                        .mapToObj(i -> bootstrap.connect("localhost", 1337).channel())
                        .collect(Collectors.toList());
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        ChannelGroup channels = new DefaultChannelGroup(eventLoopGroup.next());
        try {
            channels.add(startDistributor(eventLoopGroup));
            channels.addAll(startForwarder(eventLoopGroup));
            System.in.read();
        } finally {
            channels.close().sync();
            eventLoopGroup.shutdownGracefully();
        }
    }
}
