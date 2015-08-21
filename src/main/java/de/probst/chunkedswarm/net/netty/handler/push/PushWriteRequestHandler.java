package de.probst.chunkedswarm.net.netty.handler.push;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 20.08.15
 */
public final class PushWriteRequestHandler extends MessageToMessageEncoder<PushWriteRequest> {

    @Override
    protected void encode(ChannelHandlerContext ctx, PushWriteRequest msg, List<Object> out) throws Exception {
        out.add(msg.createChunkPushMessage(ctx.channel()));
    }
}
