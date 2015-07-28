package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIdCollectionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIdMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmId;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Channel sends to pipeline:
 * - SwarmIdCollectionEvent
 * - SwarmIdAcquisitionEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIdCollectionHandler extends ChannelHandlerAdapter {

    // The announce address, which is announced to the registration handler
    private final SocketAddress announceAddress;

    // All known swarm ids
    private final Set<SwarmId> knownSwarmIds = new HashSet<>();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The local swarm id
    private SwarmId localSwarmId;

    private void fireSwarmIdsCollected(UpdateNeighboursMessage updateNeighboursMessage) {
        ctx.pipeline().fireUserEventTriggered(new SwarmIdCollectionEvent(knownSwarmIds, updateNeighboursMessage));
    }

    private void fireSwarmIdAcquired() {
        ctx.pipeline().fireUserEventTriggered(new SwarmIdAcquisitionEvent(localSwarmId));
    }

    private void setLocalSwarmId(SetLocalSwarmIdMessage setLocalSwarmIdMessage) throws Exception {
        // Safe the new swarm id
        localSwarmId = setLocalSwarmIdMessage.getLocalSwarmId();

        // Let handler chain know, that we have acquired our swarm id
        fireSwarmIdAcquired();
    }

    private void updateNeighbours(UpdateNeighboursMessage updateNeighboursMessage) {
        // Add and remove neighbours
        knownSwarmIds.addAll(updateNeighboursMessage.getAddNeighbours());
        knownSwarmIds.removeAll(updateNeighboursMessage.getRemoveNeighbours());

        // Let handler chain know, that we have updated our set of swarm ids0
        fireSwarmIdsCollected(updateNeighboursMessage);
    }

    public SwarmIdCollectionHandler(SocketAddress announceAddress) {
        Objects.requireNonNull(announceAddress);
        this.announceAddress = announceAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;

        // Send announce port to remote
        ctx.writeAndFlush(new SetCollectorAddressMessage(announceAddress))
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean hasNotLocalSwarmId = localSwarmId == null;
        boolean isSetLocalSwarmIdMessage = msg instanceof SetLocalSwarmIdMessage;

        if (hasNotLocalSwarmId || isSetLocalSwarmIdMessage) {
            if (!hasNotLocalSwarmId || !isSetLocalSwarmIdMessage) {
                throw new IllegalStateException("!hasNotLocalSwarmId || !isSetLocalSwarmIdMessage");
            } else {
                setLocalSwarmId((SetLocalSwarmIdMessage) msg);
            }
        } else if (msg instanceof UpdateNeighboursMessage) {
            updateNeighbours((UpdateNeighboursMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
