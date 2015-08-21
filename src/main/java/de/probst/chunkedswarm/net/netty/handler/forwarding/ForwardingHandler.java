package de.probst.chunkedswarm.net.netty.handler.forwarding;

import de.probst.chunkedswarm.net.netty.handler.push.message.ChunkPushMessage;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 21.08.15
 */
public final class ForwardingHandler extends ChannelHandlerAdapter {

    private final ChannelGroup engagedForwarderChannels;

    public ForwardingHandler(ChannelGroup engagedForwarderChannels) {
        Objects.requireNonNull(engagedForwarderChannels);
        this.engagedForwarderChannels = engagedForwarderChannels;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ChunkPushMessage) {

        } else {
            super.channelRead(ctx, msg);
        }
    }
}
