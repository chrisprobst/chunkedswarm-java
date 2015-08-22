package de.probst.chunkedswarm.net.netty.handler.codec.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import de.probst.chunkedswarm.io.util.IOUtil;
import de.probst.chunkedswarm.net.netty.handler.codec.protobuf.ProtoMessages.BaseCommand;
import de.probst.chunkedswarm.net.netty.handler.codec.protobuf.ProtoMessages.BaseCommand.CommandType;
import de.probst.chunkedswarm.net.netty.handler.codec.protobuf.ProtoMessages.SerializableMsg;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.08.15
 */
public final class Message2ProtobufCodec extends MessageToMessageCodec<Message, Object> {

    @Override
    protected void decode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        ProtoMessages.BaseCommand baseCommand = (BaseCommand) msg;
        if (baseCommand.getType() != CommandType.SERIAL_MSG) {
            throw new IllegalArgumentException("Wrong message type");
        }

        SerializableMsg serializableMsg = baseCommand.getExtension(SerializableMsg.cmd);
        out.add(IOUtil.deserialize(serializableMsg.getPayload().toByteArray()));
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        byte[] arr = IOUtil.serialize(msg);

        SerializableMsg serializableMsg = ProtoMessages.SerializableMsg.newBuilder()
                                                                       .setPayload(ByteString.copyFrom(arr))
                                                                       .build();

        out.add(BaseCommand.newBuilder().setType(CommandType.SERIAL_MSG)
                           .setExtension(SerializableMsg.cmd, serializableMsg).build());
    }
}
