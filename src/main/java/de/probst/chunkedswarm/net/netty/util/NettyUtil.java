package de.probst.chunkedswarm.net.netty.util;

import com.google.protobuf.ExtensionRegistry;
import de.probst.chunkedswarm.net.netty.handler.codec.protobuf.Message2ProtobufCodec;
import de.probst.chunkedswarm.net.netty.handler.codec.protobuf.ProtoMessages;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.08.15
 */
public final class NettyUtil {

    private NettyUtil() {
    }

    public static void addCodecToPipeline(ChannelPipeline channelPipeline, int maxFrameSize) {
        ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
        ProtoMessages.registerAllExtensions(extensionRegistry);

        // Framing
        channelPipeline.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        channelPipeline.addLast(new LengthFieldPrepender(4));

        // Used to encoder/decoder protobuf messages
        channelPipeline.addLast(new ProtobufEncoder());
        channelPipeline.addLast(new ProtobufDecoder(ProtoMessages.BaseCommand.getDefaultInstance(), extensionRegistry));

        // Messages2Protobuf
        channelPipeline.addLast(new Message2ProtobufCodec());
    }
}
