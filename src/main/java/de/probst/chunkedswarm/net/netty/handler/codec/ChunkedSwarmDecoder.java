package de.probst.chunkedswarm.net.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

import java.util.List;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public class ChunkedSwarmDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int length = in.readUnsignedShort();
        out.add(CharsetUtil.UTF_8.decode(in.slice(in.readerIndex(), length).nioBuffer()));
        in.skipBytes(length);
    }
}
