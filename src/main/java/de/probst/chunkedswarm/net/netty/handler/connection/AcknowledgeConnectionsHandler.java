package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.event.AcknowledgedNeighboursEvent;
import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdRegistrationEvent;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class AcknowledgeConnectionsHandler extends ChannelHandlerAdapter {

    // Store all known neighbours here
    private final Map<String, SwarmId> knownNeighbours = new HashMap<>();

    // Store all acknowledged neighbours here
    private final Set<String> acknowledgedNeighbours = new HashSet<>();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The local swarm id
    private SwarmId localSwarmId;

    // Did we have updated our parent once ?
    private boolean hasUpdatedOnce;

    private void handleSwarmIdRegistrationEvent(SwarmIdRegistrationEvent swarmIdRegistrationEvent) {
        switch (swarmIdRegistrationEvent.getType()) {
            case Registered:
                knownNeighbours.put(swarmIdRegistrationEvent.getSwarmId().getUuid(),
                                    swarmIdRegistrationEvent.getSwarmId());
                break;
            case Unregistered:
                knownNeighbours.remove(swarmIdRegistrationEvent.getSwarmId().getUuid());
                break;
            case Acknowledged:
                knownNeighbours.put(swarmIdRegistrationEvent.getSwarmId().getUuid(),
                                    swarmIdRegistrationEvent.getSwarmId());
                break;
        }
    }

    private void handleSwarmIdAcquisitionEvent(SwarmIdAcquisitionEvent evt) {
        localSwarmId = evt.getSwarmId();
    }

    private void fireAcknowledgedNeighboursUpdateEvent() {
        Channel parent = ctx.channel().parent();
        if (parent == null) {
            throw new IllegalStateException("parent == null");
        }

        parent.pipeline()
              .fireUserEventTriggered(new AcknowledgedNeighboursEvent(AcknowledgedNeighboursEvent.Type.Update,
                                                                      knownNeighbours,
                                                                      acknowledgedNeighbours,
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
                                                                      knownNeighbours,
                                                                      acknowledgedNeighbours,
                                                                      ctx,
                                                                      localSwarmId));
    }

    private void acknowledgeNeighbours(AcknowledgeNeighboursMessage msg) {
        msg.getAddedNeighbours().forEach(acknowledgedNeighbours::add);
        msg.getRemovedNeighbours().forEach(acknowledgedNeighbours::remove);
        fireAcknowledgedNeighboursUpdateEvent();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdRegistrationEvent) {
            handleSwarmIdRegistrationEvent((SwarmIdRegistrationEvent) evt);
        } else if (evt instanceof SwarmIdAcquisitionEvent) {
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
