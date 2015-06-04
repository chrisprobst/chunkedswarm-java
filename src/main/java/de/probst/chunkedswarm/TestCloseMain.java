package de.probst.chunkedswarm;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
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

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();



        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                       .channel(NioServerSocketChannel.class)
                       .childOption(ChannelOption.TCP_NODELAY, true)
                       .childHandler(new ChannelInitializer<Channel>() {
                           @Override
                           protected void initChannel(Channel ch) throws Exception {
                               ch.pipeline().addLast(new ChannelHandlerAdapter() {

                                   @Override
                                   public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                       Runnable write = new Runnable() {
                                           @Override
                                           public void run() {
                                               ctx.channel()
                                                  .writeAndFlush(ctx.alloc().buffer(1024))
                                                  .addListener(fut -> {
                                                      if (!fut.isSuccess()) {
                                                          fut.cause().printStackTrace();
                                                      } else {
                                                          run();
                                                      }
                                                  });
                                           }
                                       };

                                       TestCloseMain.ch = ctx.channel();
                                       write.run();
                                       super.channelActive(ctx);
                                   }


                                   @Override
                                   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                       System.out.println("Server channel closed");

                                       super.channelInactive(ctx);
                                   }
                               });
                           }
                       });

        serverBootstrap.bind(1337).syncUninterruptibly();


        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                 .channel(NioSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .handler(new ChannelInitializer<Channel>() {
                     @Override
                     protected void initChannel(Channel ch) throws Exception {

                         ch.pipeline().addLast(new ChannelHandlerAdapter() {

                             @Override
                             public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                 Runnable write = new Runnable() {
                                     @Override
                                     public void run() {
                                         ctx.channel()
                                            .writeAndFlush(ctx.alloc().buffer(1024))
                                            .addListener(fut -> {
                                                if (!fut.isSuccess()) {
                                                    fut.cause().printStackTrace();
                                                } else {
                                                    run();
                                                }
                                            });
                                     }
                                 };

                                 write.run();
                                 super.channelActive(ctx);
                             }

                             @Override
                             public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                 System.out.println("Client channel closed");

                                 super.channelInactive(ctx);
                             }
                         });
                     }
                 });

        bootstrap.connect("localhost", 1337).syncUninterruptibly();

        System.in.read();

        ch.close().syncUninterruptibly();

        System.in.read();
    }
}
