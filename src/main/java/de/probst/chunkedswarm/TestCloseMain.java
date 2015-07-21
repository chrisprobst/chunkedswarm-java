package de.probst.chunkedswarm;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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
                                   public void exceptionCaught(ChannelHandlerContext ctx,
                                                               Throwable cause) throws Exception {
                                       cause.printStackTrace();
                                       System.err.println();
                                       System.err.println();
                                   }

                                   @Override
                                   public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                       ByteBuf b = ctx.alloc().buffer(1024 * 1024);
                                       for (int i = 0; i < 1024 * 256; i++) {
                                           b.writeInt(i);
                                       }


                                       Runnable write = new Runnable() {
                                           @Override
                                           public void run() {
                                               b.retain();
                                               ctx.channel()
                                                  .writeAndFlush(b.duplicate())
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

//                                   int amount = 0;
//                                   long t = System.currentTimeMillis();
//
//                                   @Override
//                                   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//                                       amount += ((ByteBuf) msg).readableBytes();
//                                       long t2 = System.currentTimeMillis();
//                                       if (t2 - t >= 1000) {
//                                           System.out.println((amount / 1024.0 / 1024.0) + " mb/sec");
//                                           amount = 0;
//                                           t = t2;
//                                       }
//
//
//                                       super.channelRead(ctx, msg);
//                                   }

                                   @Override
                                   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                       //System.out.println("Server channel closed");

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

//                             int amount = 0;
//                             long t = System.currentTimeMillis();
//
//                             @Override
//                             public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//                                 amount += ((ByteBuf) msg).readableBytes();
//                                 long t2 = System.currentTimeMillis();
//                                 if (t2 - t >= 1000) {
//                                     System.out.println((amount / 1024.0 / 1024.0) + " mb/sec");
//                                     amount = 0;
//                                     t = t2;
//                                 }
//
//
//
//                                 super.channelRead(ctx, msg);
//                             }


                             @Override
                             public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                 cause.printStackTrace();
                                 System.err.println();
                                 System.err.println();
                             }

                             @Override
                             public void channelActive(ChannelHandlerContext ctx) throws Exception {

                                 ByteBuf b = ctx.alloc().buffer(1024 * 1024);
                                 for (int i = 0; i < 1024 * 256; i++) {
                                     b.writeInt(i);
                                 }


                                 Runnable write = new Runnable() {
                                     @Override
                                     public void run() {


                                         b.retain();
                                         ctx.channel()
                                            .writeAndFlush(b.duplicate())
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
                                 //System.out.println("Client channel closed");

                                 super.channelInactive(ctx);
                             }
                         });
                     }
                 });

        ch = bootstrap.connect("kr0e.no-ip.info", 1337).syncUninterruptibly().channel();

        System.in.read();

        ch.close().awaitUninterruptibly();

        System.in.read();
    }
}
