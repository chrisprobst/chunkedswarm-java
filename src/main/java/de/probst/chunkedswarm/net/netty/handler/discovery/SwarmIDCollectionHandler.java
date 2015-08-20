package de.probst.chunkedswarm.net.netty.handler.discovery;

import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDAcquisitionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.event.SwarmIDCollectionEvent;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetCollectorAddressMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.SetLocalSwarmIDMessage;
import de.probst.chunkedswarm.net.netty.handler.discovery.message.UpdateNeighboursMessage;
import de.probst.chunkedswarm.util.SwarmID;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Handler sends to owner channel:
 * - SwarmIDCollectionEvent
 * - SwarmIDAcquisitionEvent
 *
 * @author Christopher Probst <christopher.probst@hhu.de>
 * @version 1.0, 22.05.15
 */
public final class SwarmIDCollectionHandler extends ChannelHandlerAdapter {

    // The announce address, which is announced to the registration handler
    private final SocketAddress announceAddress;

    // All known swarm ids
    private final Set<SwarmID> knownSwarmIDs = new HashSet<>();

    // The channel handler context
    private ChannelHandlerContext ctx;

    // The local swarm id
    private SwarmID localSwarmID;

    private void fireSwarmIDsCollected(UpdateNeighboursMessage msg) {
        ctx.pipeline().fireUserEventTriggered(new SwarmIDCollectionEvent(knownSwarmIDs, msg));
    }

    private void fireSwarmIDAcquired() {
        ctx.pipeline().fireUserEventTriggered(new SwarmIDAcquisitionEvent(localSwarmID));
    }

    private void setLocalSwarmID(SetLocalSwarmIDMessage msg) {
        // Safe the new swarm id
        localSwarmID = msg.getLocalSwarmID();

        // Let handler chain know, that we have acquired our swarm id
        fireSwarmIDAcquired();
    }

    private void updateNeighbours(UpdateNeighboursMessage msg) {
        // Add and remove neighbours
        knownSwarmIDs.addAll(msg.getAddNeighbours());
        knownSwarmIDs.removeAll(msg.getRemoveNeighbours());

        // Let handler chain know, that we have updated our set of swarm ids0
        fireSwarmIDsCollected(msg);
    }

    public SwarmIDCollectionHandler(SocketAddress announceAddress) {
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
        boolean hasNotLocalSwarmID = localSwarmID == null;
        boolean isSetLocalSwarmIDMessage = msg instanceof SetLocalSwarmIDMessage;

        if (hasNotLocalSwarmID || isSetLocalSwarmIDMessage) {
            if (!hasNotLocalSwarmID || !isSetLocalSwarmIDMessage) {
                throw new IllegalStateException("!hasNotLocalSwarmID || !isSetLocalSwarmIDMessage");
            } else {
                setLocalSwarmID((SetLocalSwarmIDMessage) msg);
            }
        } else if (msg instanceof UpdateNeighboursMessage) {
            updateNeighbours((UpdateNeighboursMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
