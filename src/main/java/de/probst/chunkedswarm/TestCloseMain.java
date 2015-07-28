package de.probst.chunkedswarm;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 04.06.15
 */
public class TestCloseMain {

    static volatile Channel ch;

    public static void main(String[] args) throws IOException {

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);

        ChannelGroup cg = new DefaultChannelGroup(eventLoopGroup.next());

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .handler(new ChannelHandlerAdapter() {


                           @Override
                           public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                               System.out.println(msg.getClass() + " -> " + msg);

                               System.out.println("Happened before");
                               cg.close();

                               super.channelRead(ctx, msg);
                           }
                       })
                       .childOption(ChannelOption.TCP_NODELAY, true)
                       .childHandler(new ChannelInitializer<Channel>() {
                           @Override
                           protected void initChannel(Channel ch) throws Exception {
                               ch.pipeline().addLast(new ChannelHandlerAdapter() {
                                   @Override
                                   public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                       cg.add(ctx.channel());
                                       System.out.println("Still active! " + ctx.channel().parent().isOpen());
                                       super.channelActive(ctx);
                                   }

                                   @Override
                                   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                       System.out.println("Never happens");
                                       super.channelInactive(ctx);
                                   }
                               });
                           }
                       });

        cg.add(serverBootstrap.bind(1337).syncUninterruptibly().channel());

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<Channel>() {
                     @Override
                     protected void initChannel(Channel ch) throws Exception {

                     }
                 });

        ch = bootstrap.connect("localhost", 1337).syncUninterruptibly().channel();

        System.in.read();
        ch.close().awaitUninterruptibly();
    }
}
