package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDAcquisitionEvent;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Handler sends to parent channel:
 * - AcknowledgedNeighboursEvent
 * <p>
 * Handler listens to:
 * - SwarmIdAcquisitionEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class AcknowledgeConnectionsHandler extends ChannelHandlerAdapter {

    // Store all acknowledged outbound neighbours here
    private final Set<UUID> acknowledgedOutboundNeighbours = new HashSet<>();

    // Store all acknowledged inbound neighbours here
    private final Set<UUID> acknowledgedInboundNeighbours = new HashSet<>();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The local swarm id
    private SwarmID localSwarmID;

    // Whether or not we are registered
    private boolean registered;

    private void fireRegisterAcknowledgedNeighboursEvent() {
        Channel parent = ctx.channel().parent();
        if (parent == null) {
            throw new IllegalStateException("parent == null");
        }

        parent.pipeline()
              .fireUserEventTriggered(new AcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent.Type.Register,
                                                                      acknowledgedOutboundNeighbours,
                                                                      acknowledgedInboundNeighbours,
                                                                      Optional.empty(),
                                                                      localSwarmID,
                                                                      ctx.channel()));

        // We are registered now
        registered = true;
    }

    private void fireUpdateAcknowledgedNeighboursEvent(AcknowledgeNeighboursMessage msg) {
        if (!registered) {
            return;
        }

        Channel parent = ctx.channel().parent();
        if (parent == null) {
            throw new IllegalStateException("parent == null");
        }

        parent.pipeline()
              .fireUserEventTriggered(new AcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent.Type.Update,
                                                                      acknowledgedOutboundNeighbours,
                                                                      acknowledgedInboundNeighbours,
                                                                      Optional.of(msg),
                                                                      localSwarmID,
                                                                      ctx.channel()));
    }

    private void fireUnregisterAcknowledgedNeighboursEvent() {
        if (!registered) {
            return;
        }

        Channel parent = ctx.channel().parent();
        if (parent == null) {
            throw new IllegalStateException("parent == null");
        }

        parent.pipeline()
              .fireUserEventTriggered(new AcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent.Type.Unregister,
                                                                      acknowledgedOutboundNeighbours,
                                                                      acknowledgedInboundNeighbours,
                                                                      Optional.empty(),
                                                                      localSwarmID,
                                                                      ctx.channel()));
    }

    private void handleSwarmIDAcquisitionEvent(SwarmIDAcquisitionEvent evt) {
        // Acquire local swarm id
        localSwarmID = evt.getSwarmID();

        // Register
        fireRegisterAcknowledgedNeighboursEvent();
    }

    private void acknowledgeNeighbours(AcknowledgeNeighboursMessage msg) {
        msg.getAddedOutboundNeighbours().forEach(acknowledgedOutboundNeighbours::add);
        msg.getRemovedOutboundNeighbours().forEach(acknowledgedOutboundNeighbours::remove);
        msg.getAddedInboundNeighbours().forEach(acknowledgedInboundNeighbours::add);
        msg.getRemovedInboundNeighbours().forEach(acknowledgedInboundNeighbours::remove);
        fireUpdateAcknowledgedNeighboursEvent(msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIDAcquisitionEvent) {
            handleSwarmIDAcquisitionEvent((SwarmIDAcquisitionEvent) evt);
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
        fireUnregisterAcknowledgedNeighboursEvent();
        super.channelInactive(ctx);
    }
}
