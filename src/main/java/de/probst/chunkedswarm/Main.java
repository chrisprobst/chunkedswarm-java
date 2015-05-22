package de.probst.chunkedswarm;

import de.probst.chunkedswarm.distributor.Distributor;
import de.probst.chunkedswarm.net.netty.handler.app.ForwarderHandler;
import de.probst.chunkedswarm.net.netty.handler.codec.SimpleCodec;
import de.probst.chunkedswarm.net.netty.handler.discovery.SwarmIdCollectionHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 19.05.15
 */
public class Main {


    private static List<Channel> startForwarder(EventLoopGroup eventLoopGroup) throws InterruptedException {
        AtomicInteger portCounter = new AtomicInteger(20000);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         //ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
                         ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                         ch.pipeline().addLast(new LengthFieldPrepender(4));
                         ch.pipeline().addLast(new SimpleCodec());


                         ch.pipeline().addLast(new SwarmIdCollectionHandler(portCounter.getAndIncrement()));

                         ch.pipeline().addLast(new ForwarderHandler());
                     }
                 });

        return IntStream.range(0, 4)
                        .mapToObj(i -> bootstrap.connect("localhost", 1337).channel())
                        .collect(Collectors.toList());
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        ChannelGroup channels = new DefaultChannelGroup(eventLoopGroup.next());
        try {
            Distributor distributor = new Distributor(eventLoopGroup);

            channels.add(distributor.getServerChannel());
            channels.addAll(startForwarder(eventLoopGroup));
            System.in.read();
        } finally {
            channels.close().sync();
            eventLoopGroup.shutdownGracefully();
        }
    }
}
