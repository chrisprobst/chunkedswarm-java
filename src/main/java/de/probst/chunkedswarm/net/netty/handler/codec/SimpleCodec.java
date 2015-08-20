package de.probst.chunkedswarm.net.netty.handler.codec;

import de.probst.chunkedswarm.io.util.IoUtil;
import de.probst.chunkedswarm.net.netty.handler.push.message.ChunkPushMessage;
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

    private static final byte SERIALIZABLE = 0;
    private static final byte BYTEBUF = 1;
    private static final byte CHUNK = 2;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        if (msg instanceof ChunkPushMessage) {
            ChunkPushMessage chunkPushMessage = (ChunkPushMessage) msg;
            ByteBuf header = Unpooled.buffer(1).writeByte(CHUNK);
            ByteBuf blockHeader = Unpooled.wrappedBuffer(IoUtil.serialize(chunkPushMessage.getBlock()));
            ByteBuf chunkHeader = Unpooled.wrappedBuffer(IoUtil.serialize(chunkPushMessage.getChunk()));
            ByteBuf payload = Unpooled.wrappedBuffer(chunkPushMessage.getChunkPayload());
            ByteBuf total = Unpooled.wrappedBuffer(header, blockHeader, chunkHeader, payload);
            //System.out.println(total.readableBytes());
            out.add(total);
        } else if (msg instanceof Serializable) {
            out.add(Unpooled.wrappedBuffer(Unpooled.buffer(1).writeByte(SERIALIZABLE),
                                           Unpooled.wrappedBuffer(IoUtil.serialize(msg))));
        } else {
            out.add(Unpooled.wrappedBuffer(Unpooled.buffer(1).writeByte(BYTEBUF), ((ByteBuf) msg).retain()));
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        if (msg.readByte() == SERIALIZABLE) {
            byte[] arr = new byte[msg.readableBytes()];
            msg.readBytes(arr);
            out.add(IoUtil.deserialize(arr));
        } else {
            msg.retain();
            out.add(msg);
        }
    }
}
