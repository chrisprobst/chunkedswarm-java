package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.message.SetForwarderSwarmIDMessage;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 29.07.15
 */
public final class ForwarderConnectionHandler extends ChannelHandlerAdapter {

    private final SwarmID localSwarmID;

    public ForwarderConnectionHandler(SwarmID localSwarmID) {
        Objects.requireNonNull(localSwarmID);
        this.localSwarmID = localSwarmID;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Send local swarm id to remote
        ctx.writeAndFlush(new SetForwarderSwarmIDMessage(localSwarmID))
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        super.channelActive(ctx);
    }
}
