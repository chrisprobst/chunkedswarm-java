package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAcquisitionEvent;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Handler sends to parent channel:
 * - AcknowledgedNeighboursEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class AcknowledgeConnectionsHandler extends ChannelHandlerAdapter {

    // Store all acknowledged outbound neighbours here
    private final Set<String> acknowledgedOutboundNeighbours = new HashSet<>();

    // Store all acknowledged inbound neighbours here
    private final Set<String> acknowledgedInboundNeighbours = new HashSet<>();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The local swarm id
    private SwarmId localSwarmId;

    // Did we have updated our parent once ?
    private boolean hasUpdatedOnce;

    private void fireAcknowledgedNeighboursUpdateEvent(AcknowledgeNeighboursMessage msg) {
        Channel parent = ctx.channel().parent();
        if (parent == null) {
            throw new IllegalStateException("parent == null");
        }

        parent.pipeline()
              .fireUserEventTriggered(new AcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent.Type.Update,
                                                                      acknowledgedOutboundNeighbours,
                                                                      acknowledgedInboundNeighbours,
                                                                      Optional.of(msg),
                                                                      ctx,
                                                                      localSwarmId));

        hasUpdatedOnce = true;
    }

    private void fireAcknowledgedNeighboursDisposeEvent() {
        if (!hasUpdatedOnce) {
            return;
        }

        Channel parent = ctx.channel().parent();
        if (parent == null) {
            throw new IllegalStateException("parent == null");
        }

        parent.pipeline()
              .fireUserEventTriggered(new AcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent.Type.Dispose,
                                                                      acknowledgedOutboundNeighbours,
                                                                      acknowledgedInboundNeighbours,
                                                                      Optional.empty(),
                                                                      ctx,
                                                                      localSwarmId));
    }

    private void handleSwarmIdAcquisitionEvent(SwarmIdAcquisitionEvent evt) {
        localSwarmId = evt.getSwarmId();
    }

    private void acknowledgeNeighbours(AcknowledgeNeighboursMessage msg) {
        msg.getAddedOutboundNeighbours().forEach(acknowledgedOutboundNeighbours::add);
        msg.getRemovedOutboundNeighbours().forEach(acknowledgedOutboundNeighbours::remove);
        msg.getAddedInboundNeighbours().forEach(acknowledgedInboundNeighbours::add);
        msg.getRemovedInboundNeighbours().forEach(acknowledgedInboundNeighbours::remove);
        fireAcknowledgedNeighboursUpdateEvent(msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdAcquisitionEvent) {
            handleSwarmIdAcquisitionEvent((SwarmIdAcquisitionEvent) evt);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof AcknowledgeNeighboursMessage) {
            acknowledgeNeighbours((AcknowledgeNeighboursMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fireAcknowledgedNeighboursDisposeEvent();
        super.channelInactive(ctx);
    }
}
