package de.probst.chunkedswarm.net.netty.handler.group;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;

import java.util.Objects;

/**
 * Sharable channel group handler.
 * <p>
 * Put this handle into one or more channel
 * pipelines and all of those channels will be
 * added to and removed from the internal channel
 * group.
 * <p>
 * Created by chrisprobst on 12.08.14.
 */
@ChannelHandler.Sharable
public final class ChannelGroupHandler extends ChannelHandlerAdapter {

    private final ChannelGroup channelGroup;

    public ChannelGroupHandler(ChannelGroup channelGroup) {
        Objects.requireNonNull(channelGroup);
        this.channelGroup = channelGroup;
    }

    /**
     * @return The channel group.
     */
    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channelGroup.add(ctx.channel());
        super.channelActive(ctx);
    }
}
