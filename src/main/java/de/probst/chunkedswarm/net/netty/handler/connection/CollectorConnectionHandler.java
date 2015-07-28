package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.NeighbourConnectionEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.SetForwarderSwarmIdMessage;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

/**
 * Channel sends to report pipeline:
 * - NeighbourConnectionEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 28.07.15
 */
public final class CollectorConnectionHandler extends ChannelHandlerAdapter {

    // The report channel
    private final Channel reportChannel;

    // The channel handler context
    private ChannelHandlerContext ctx;

    private SwarmId forwarderSwarmId;

    private void fireChannelConnected() {
        if (forwarderSwarmId == null) {
            throw new IllegalStateException("forwarderSwarmId == null");
        }
        reportChannel.pipeline()
                     .fireUserEventTriggered(new NeighbourConnectionEvent(forwarderSwarmId,
                                                                          ctx.channel(),
                                                                          NeighbourConnectionEvent.Direction.Inbound,
                                                                          NeighbourConnectionEvent.Type.Connected));
    }

    private void fireChannelDisconnected() {
        if (forwarderSwarmId == null) {
            return;
        }

        reportChannel.pipeline()
                     .fireUserEventTriggered(new NeighbourConnectionEvent(forwarderSwarmId,
                                                                          ctx.channel(),
                                                                          NeighbourConnectionEvent.Direction.Inbound,
                                                                          NeighbourConnectionEvent.Type.Disconnected));
    }

    private void setForwarderSwarmId(SetForwarderSwarmIdMessage msg) {
        // Safe the new swarm id
        forwarderSwarmId = msg.getForwarderSwarmId();

        // Let handler chain know, that we have acquired our swarm id
        fireChannelConnected();
    }

    public CollectorConnectionHandler(Channel reportChannel) {
        Objects.requireNonNull(reportChannel);
        this.reportChannel = reportChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fireChannelDisconnected();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean hasNotForwarderSwarmId = forwarderSwarmId == null;
        boolean isSetForwarderSwarmIdMessage = msg instanceof SetForwarderSwarmIdMessage;

        if (hasNotForwarderSwarmId || isSetForwarderSwarmIdMessage) {
            if (!hasNotForwarderSwarmId || !isSetForwarderSwarmIdMessage) {
                throw new IllegalStateException("!hasNotForwarderSwarmId || !isSetForwarderSwarmIdMessage");
            } else {
                setForwarderSwarmId((SetForwarderSwarmIdMessage) msg);
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
