package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.ConnectionChangeEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.SetForwarderSwarmIDMessage;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

/**
 * Handler sends to report channel:
 * - ConnectionChangeEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 28.07.15
 */
public final class CollectorConnectionHandler extends ChannelHandlerAdapter {

    // The report channel
    private final Channel reportChannel;

    // The channel handler context
    private ChannelHandlerContext ctx;

    private SwarmID forwarderSwarmID;

    private void fireInboundConnected() {
        if (forwarderSwarmID == null) {
            throw new IllegalStateException("forwarderSwarmID == null");
        }
        reportChannel.pipeline()
                     .fireUserEventTriggered(new ConnectionChangeEvent(forwarderSwarmID,
                                                                       ctx.channel(),
                                                                       ConnectionChangeEvent.Direction.Inbound,
                                                                       ConnectionChangeEvent.Type.Connected));
    }

    private void fireInboundDisconnected() {
        if (forwarderSwarmID == null) {
            return;
        }

        reportChannel.pipeline()
                     .fireUserEventTriggered(new ConnectionChangeEvent(forwarderSwarmID,
                                                                       ctx.channel(),
                                                                       ConnectionChangeEvent.Direction.Inbound,
                                                                       ConnectionChangeEvent.Type.Disconnected));
    }

    private void setForwarderSwarmID(SetForwarderSwarmIDMessage msg) {
        // Safe the new swarm id
        forwarderSwarmID = msg.getForwarderSwarmID();

        // Let handler chain know, that we have acquired our swarm id
        fireInboundConnected();
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
        fireInboundDisconnected();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean hasNotForwarderSwarmID = forwarderSwarmID == null;
        boolean isSetForwarderSwarmIDMessage = msg instanceof SetForwarderSwarmIDMessage;

        if (hasNotForwarderSwarmID || isSetForwarderSwarmIDMessage) {
            if (!hasNotForwarderSwarmID || !isSetForwarderSwarmIDMessage) {
                throw new IllegalStateException("!hasNotForwarderSwarmID || !isSetForwarderSwarmIDMessage");
            } else {
                setForwarderSwarmID((SetForwarderSwarmIDMessage) msg);
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
