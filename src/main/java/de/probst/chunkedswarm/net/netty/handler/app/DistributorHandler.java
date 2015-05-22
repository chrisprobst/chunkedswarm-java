package de.probst.chunkedswarm.net.netty.handler.app;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public class DistributorHandler extends ChannelHandlerAdapter {



    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
