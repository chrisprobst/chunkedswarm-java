package de.probst.chunkedswarm.net.netty.handler.connection;

import de.probst.chunkedswarm.net.netty.handler.connection.message.AcknowledgeNeighboursMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAcquisitionEvent;
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

    private final Set<String> acknowledged = new HashSet<>();

    SwarmId swarmId;

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SwarmIdAcquisitionEvent) {
            swarmId = ((SwarmIdAcquisitionEvent) evt).getSwarmId();
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
