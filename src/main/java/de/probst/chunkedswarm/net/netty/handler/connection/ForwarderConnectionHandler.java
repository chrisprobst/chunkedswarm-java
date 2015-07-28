package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.message.SetForwarderSwarmIdMessage;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 29.07.15
 */
public final class ForwarderConnectionHandler extends ChannelHandlerAdapter {

    private final SwarmId localSwarmId;

    public ForwarderConnectionHandler(SwarmId localSwarmId) {
        Objects.requireNonNull(localSwarmId);
        this.localSwarmId = localSwarmId;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Send local swarm id to remote
        ctx.writeAndFlush(new SetForwarderSwarmIdMessage(localSwarmId))
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        super.channelActive(ctx);
    }
}
