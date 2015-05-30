package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdRegistrationEvent;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 30.05.15
 */
public final class AcknowledgeConnectionsHandler extends ChannelHandlerAdapter {

    // Store all known neighbours here
    private final Set<SwarmId> knownNeighbours = new HashSet<>();

    // Store all acknowledged neighbours here
    private final Set<String> acknowledgedNeighbours = new HashSet<>();

    private void handleSwarmIdRegistrationEvent(SwarmIdRegistrationEvent swarmIdRegistrationEvent) {
        switch (swarmIdRegistrationEvent.getType()) {
            case Registered:
                knownNeighbours.add(swarmIdRegistrationEvent.getSwarmId());
                break;
            case Unregistered:
                knownNeighbours.remove(swarmIdRegistrationEvent.getSwarmId());
                break;
            case Acknowledged:
                knownNeighbours.add(swarmIdRegistrationEvent.getSwarmId());
                break;
        }
    }

    private void acknowledgeNeighbours(AcknowledgeNeighboursMessage msg) {
        msg.getAddedNeighbours().forEach(acknowledgedNeighbours::add);
        msg.getRemovedNeighbours().forEach(acknowledgedNeighbours::remove);
        System.out.println(this + " -> " + acknowledgedNeighbours.size());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdRegistrationEvent) {
            handleSwarmIdRegistrationEvent((SwarmIdRegistrationEvent) evt);
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
}
