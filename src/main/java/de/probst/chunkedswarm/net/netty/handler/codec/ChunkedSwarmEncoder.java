package de.probst.chunkedswarm.net.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;

import java.nio.ByteBuffer;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public class ChunkedSwarmEncoder extends MessageToByteEncoder<Object> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        ByteBuffer utf8String = CharsetUtil.UTF_8.encode(msg.toString());
        out.writeShort(utf8String.remaining());
        out.writeBytes(utf8String);
    }
}
