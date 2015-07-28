package de.probst.chunkedswarm.net.netty.handler.codec;

import de.probst.chunkedswarm.io.util.IoUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.io.Serializable;
import java.util.List;

/**
 * A simple ByteBuf/Serializable Codec.
 * <p>
 * Raw bytes are going straight through,
 * objects are serialized.
 * <p>
 * Created by chrisprobst on 31.08.14.
 */
public final class SimpleCodec extends MessageToMessageCodec<ByteBuf, Object> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        if (msg instanceof Serializable) {
            out.add(Unpooled.wrappedBuffer(Unpooled.buffer(1).writeByte(0),
                                           Unpooled.wrappedBuffer(IoUtil.serialize(msg))));
        } else {
            out.add(Unpooled.wrappedBuffer(Unpooled.buffer(1).writeByte(1), ((ByteBuf) msg).retain()));
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        if (msg.readByte() == 0) {
            byte[] arr = new byte[msg.readableBytes()];
            msg.readBytes(arr);
            out.add(IoUtil.deserialize(arr));
        } else {
            msg.retain();
            out.add(msg);
        }
    }
}
