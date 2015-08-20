package de.probst.chunkedswarm.net.netty.handler.app;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public class ForwarderHandler extends ChannelHandlerAdapter {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("Closing forwarder2distributor channel. Cause: " + cause);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        if(msg instanceof String) {
//            System.out.println("Received push: " + msg);
//        }
        super.channelRead(ctx, msg);
    }
}
